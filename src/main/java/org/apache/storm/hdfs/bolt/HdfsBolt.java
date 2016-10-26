package org.apache.storm.hdfs.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.common.rotation.RotationAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

public class HdfsBolt extends AbstractHdfsBolt {
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(HdfsBolt.class);

	private transient FSDataOutputStream out;
	private RecordFormat format;
	private long offset = 0;
	private long rotationOffset = 0;
	private long writeFailureCount = 0;
	private long fileRotationFailureCount = 0;
	private Path currentFilePath;

	private static final long MAX_FAILURE_COUNT = 3;

	public HdfsBolt withRecordFormat(RecordFormat format) {
		this.format = format;
		return this;
	}

	public HdfsBolt withFsUrl(String fsUrl) {
		this.fsUrl = fsUrl;
		return this;
	}

	public HdfsBolt withRotationPolicy(FileRotationPolicy rotationPolicy) {
		this.rotationPolicy = rotationPolicy;
		return this;
	}

	public HdfsBolt withSyncPolicy(SyncPolicy syncPolicy) {
		this.syncPolicy = syncPolicy;
		return this;
	}

	public HdfsBolt withFileNameFormat(FileNameFormat fileNameFormat) {
		this.fileNameFormat = fileNameFormat;
		return this;
	}

	@Override
	public void doPrepare(Map conf, TopologyContext topologyContext, OutputCollector collector) throws IOException {
		this.fs = FileSystem.get(URI.create(this.fsUrl), hdfsConfig);
	}

	@Override
	public void execute(Tuple tuple) {
		try {
			byte[] bytes = tuple.getString(0).getBytes();
			synchronized (this.writeLock)
			{
				this.out.write(bytes);
				this.offset += bytes.length;
				this.rotationOffset += bytes.length;

				if (this.syncPolicy.mark(tuple, this.offset))
				{
					if (this.out instanceof HdfsDataOutputStream)
					{
						((HdfsDataOutputStream) this.out).hsync(EnumSet.of(SyncFlag.UPDATE_LENGTH));
					} else
					{
						this.out.hsync();
					}
					this.syncPolicy.reset();
					this.offset = 0;
				}
			}

			// This can cause data loss if fs.azure.write.request.size is set to default value.
			// In this case, WASB driver will buffer up to 4 MB before flushing. If the bolt thread dies in between,
			// we have data loss.
			this.collector.ack(tuple);

		} catch (IOException e) {
			writeFailureCount++;
			LOG.warn("write/sync failed. writeFailureCount = " + writeFailureCount, e);
			this.collector.fail(tuple);
		}

		try {
			// Rotate the file if we have hit the rotationPolicy or encountered
			// enough write failures
			if (writeFailureCount >= MAX_FAILURE_COUNT || this.rotationPolicy.mark(tuple, this.rotationOffset))
			{
				if (writeFailureCount >= MAX_FAILURE_COUNT)
				{
					LOG.warn("Failed to write " + writeFailureCount + " times, rotating file now.");
				}

				rotateOutputFile();
				this.rotationOffset = 0;
				this.rotationPolicy.reset();

				// Now that we were able to successfully rotate the file, lets reset writeFailureCount
				writeFailureCount = 0;
			}
		} catch (IOException e) {
			fileRotationFailureCount++;
			LOG.warn("File rotation failed. fileRotationFailureCount = " + fileRotationFailureCount, e);
		}

		if (fileRotationFailureCount >= MAX_FAILURE_COUNT) {
			LOG.info("Failed to rotate file " + fileRotationFailureCount + " times, erroring out now.");
			throw new RuntimeException("Failed to rotate file despite multiple retries.");
		}
	}

	@Override
	protected void closeOutputFile() throws IOException {
		this.out.close();
	}

	@Override
	protected Path createOutputFile() throws IOException {
		Path filePath = new Path(this.fileNameFormat.getPath(),
				this.fileNameFormat.getName(this.rotation, System.currentTimeMillis()));
		//LOG.info("Using output file: " + filePath.getName());
		this.out = this.fs.create(filePath);
		return filePath;
	}
}

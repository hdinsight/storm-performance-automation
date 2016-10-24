package org.apache.storm.hdfs;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.HourlyFileNameFormat;
import org.apache.storm.hdfs.bolt.WasbBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SizeSyncPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import com.beust.jcommander.*;


public class WriteBufferTopology {

	private static final Logger LOG = LoggerFactory.getLogger(WriteBufferTopology.class);

	//Input Parameters
	@Parameter(names={"-workers","-w"}, description="Number of Worker Processes")
	static Integer workers = 0;

	@Parameter(names={"-recordSize","-x"}, description="Size of record being written in bytes.")
	static Integer recordSize = 0;

	@Parameter(names={"-spoutParallelism","-s"}, description="Number of spout instances across all worker processes.")
	static Integer spoutParallelism = 0;

	@Parameter(names={"-boltParallelism","-b"}, description="Number of bolt instances across all worker processes.")
	static Integer boltParallelism = 0;

	@Parameter(names={"-fileRotationSize","-f"}, description="Size at which the file being written to is rotated.")
	static Integer fileRotationSize = 0;

	@Parameter(names={"-fileBufferSize","-z"}, description="The size of the buffer in bytes. Messages are buffered to this size before being flushed.")
	static Integer fileBufferSize = 0;

	@Parameter(names={"-numRecords","-n"}, description="Number of records written by each instance of the bolt.")
	static Integer numRecords = 0;

	@Parameter(names={"-maxSpoutPending","-p"}, description="Max number of records alive in the topology that have not yet been acked.")
	static Integer maxSpoutPending = 0;

	@Parameter(names={"-topologyName","-y"}, description="Name of the topology name.")
	static String topologyName = "Storm_Perf_Toplogy";

	@Parameter(names={"-storageUrl","-u"}, description="Storage Url. (WASB/ADLS)")
	static String storageUrl;

	@Parameter(names={"-storageFileDirPath","-r"}, description="")
	static String storageFileDirPath;

	@Parameter(names={"-numTasksBolt","-t"}, description="Number of tasks executed by each bolt thread.")
	static Integer numTasksBolt = 0;

	@Parameter(names={"-numTasksSpout","-e"}, description="Number of tasks executed by each spout thread.")
	static Integer numTasksSpout = 0;

	@Parameter(names={"-numAckers","-k"}, description="Number of ackers.")
	static Integer numAckers = 0;

	@Parameter(names={"-sizeSyncPolicyEnabled","-v"}, description="Enable Size Sync Policy.")
	static boolean sizeSyncPolicyEnabled = false;

	public static void main(String[] args) throws Exception
	{
		// Parse Input parameters
		WriteBufferTopology writeBufferTopology = new WriteBufferTopology();
		JCommander cmdLineParser = new JCommander(writeBufferTopology,args);

		System.out.println("Using: ");
		System.out.println("Topology Name: " + topologyName);
		System.out.println("Number of workers: " + workers);
		System.out.println("Message size (B): " + recordSize);
		System.out.println("Number of records per bolt: " + numRecords);
		System.out.println("Spout Parallelism: " + spoutParallelism);
		System.out.println("Bolt Parallelism: " + boltParallelism);
		System.out.println("Number of ackers: " + numAckers);
		System.out.println("Number of tasks per bolt: " + numTasksBolt);
		System.out.println("Number of tasks per spout: " + numTasksSpout);
		System.out.println("Max Spout Pending: " + maxSpoutPending);
		System.out.println("File Rotation size (MB): " + fileRotationSize);
		System.out.println("Client size buffer size: (B) " + fileBufferSize);
		System.out.println("Storage File Repo" + storageUrl);
		System.out.println("Storage File container: " + storageFileDirPath);
		System.out.println("Size Sync Policy Enabled: " + sizeSyncPolicyEnabled);

		// Build Topology
		TopologyBuilder builder = new TopologyBuilder();
		TestSpout randomSeqSpout = new TestSpout(recordSize, numRecords, topologyName, spoutParallelism/workers);

		builder.setSpout("testgenerator", randomSeqSpout, spoutParallelism)
				.setNumTasks(numTasksSpout);

		FileNameFormat fileNameFormat2 = new HourlyFileNameFormat()
				.withPath(storageFileDirPath);

		WasbBolt wasbBolt = new WasbBolt()
				.withRecordFormat(
						new DelimitedRecordFormat().withFieldDelimiter(","))
				.withFsUrl(storageUrl)
				.withRotationPolicy(new FileSizeRotationPolicy(fileRotationSize, Units.MB))
				.withSyncPolicy(new SizeSyncPolicy(fileBufferSize, sizeSyncPolicyEnabled))
				.withFileNameFormat(fileNameFormat2);

		builder.setBolt("hdfsBolt", wasbBolt, boltParallelism)
				.setNumTasks(numTasksBolt)
				.localOrShuffleGrouping("testgenerator");

		// Set configurations
		Config conf = new Config();
		conf.setNumWorkers(workers);
		conf.setNumAckers(numAckers);
		conf.setMaxSpoutPending(maxSpoutPending);
		conf.put(Config.WORKER_CHILDOPTS, "-Xmx16g");
		conf.setDebug(false);

		//Submit Topology
		StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
	}
}


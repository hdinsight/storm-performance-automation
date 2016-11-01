package org.apache.storm.hdfs;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.generated.KillOptions;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.utils.NimbusClient;
import backtype.storm.generated.NotAliveException;
import backtype.storm.utils.Utils;
import java.util.List;
import java.io.FileWriter;
import java.lang.String;
import java.util.Map;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.io.PrintWriter;
import java.util.UUID;
import java.util.concurrent.*;
import java.io.File;
import java.io.BufferedWriter;
import java.lang.StringBuilder;
import java.util.Random;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import java.io.UnsupportedEncodingException;
import org.apache.thrift7.TException;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RandomSequenceSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(RandomSequenceSpout.class);
	SpoutOutputCollector _collector;
	long maxCount;
	boolean currentThreadMarkedComplete = false;
	long spoutThreads = 0;
	long currCount = 0;
	long seqId = 0;
	long length;
	Random r = new Random();
	String alphaNum = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	List<Object> currentTuple;
	String zookeeperConnString;
	long startTime;
	String topologyName;
	File resultsFile;
	boolean spoutThreadComplete = false;
	public static ZookeeperClient zkClient;

	public RandomSequenceSpout(int sizeInBytes, int maxCount, String topologyName, long spoutThreads, String zkConnStr) 
	{
		this.spoutThreads = spoutThreads;
		this.maxCount = maxCount;
		this.startTime = System.currentTimeMillis();
		this.length = sizeInBytes;
		this.zookeeperConnString = zkConnStr;
		this.topologyName = topologyName;
		this.resultsFile = new File("/tmp/" + topologyName + ".txt");		
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
		_collector.emit(this.currentTuple, id);
	}

	public void nextTuple() 
	{
		if(currCount <= maxCount)
		{
			Object id = Long.toString(seqId);
			currentTuple = new Values(getRandomSeqValue());
			_collector.emit(currentTuple, id);

			// Write results for current thread to output file
			if (currCount % (maxCount/10) == 0)
			{
				log("Processed " + currCount + "records");
				writeProgressToFile();
			}

			currCount++;
			seqId++;
		}
		else
		{
			if (!spoutThreadComplete)
			{
				this.zkClient = new ZookeeperClient(zookeeperConnString);
				// Attempt to increment shared counter in Zookeeper
				log("Attempting to update shared counter in Zookeeper.");
				if(this.zkClient.updatedSharedWorkProgressCounter())
				{
					spoutThreadComplete = true;
					log("Marking thread as complete.");
				}
			}
			else
			{
				long completedThreads = this.zkClient.getSharedCounterValue();
				log("Checking if all other threads have completed work.");
				log("Completed Threads: " + completedThreads + ", Spout Threads: " + this.spoutThreads);
				if (completedThreads == this.spoutThreads)
				{
					log("Killing topology");
					// Delete Zookeeper paths used
					this.zkClient.deletePaths();
					// Close zkClient
					this.zkClient.close();
					killTopology();
				}
			}

			// Sleep for 10s while waiting for other threads to complete.
			// Last thread to complete work will kill the topology
			try 
			{
				log("Sleeping for 10 seconds..");
				Thread.sleep(10000);
			} 
			catch (InterruptedException ex) 
			{
    			Thread.currentThread().interrupt();
			}
		}
	}

	public void writeProgressToFile()
	{
		long elapsedTime = System.currentTimeMillis() - this.startTime;
		String timeStamp = new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime());
		try
		{
			if(!this.resultsFile.exists())
			{
				this.resultsFile.createNewFile();
			}

			FileWriter fw = new FileWriter(this.resultsFile.getAbsoluteFile(), true);
			BufferedWriter bw = new BufferedWriter(fw);
			if (currCount == 0)
			{
				bw.write("ThreadId,Timestamp,RecordsProcessed,TimeElapsedInMs\n");
			}
		    bw.write(getContextId() + "," + timeStamp + "," + currCount + "," + Long.toString(elapsedTime) + "\n"); 
		    bw.close();
		}
		catch(IOException ioe)
		{
		    log("IOException: " + ioe.getMessage());
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tick"));
	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
	}

	public String getRandomSeqValue()
	{
		StringBuilder str = new StringBuilder();
		str.append(Long.toString(seqId) + ":'");
		int numDigitsInSeqId = seqId==0?1:(int)(Math.log10(seqId)+1);
		byte[] ch = new byte[(int)(length - numDigitsInSeqId -4)];
		for (int i=0; i < ch.length; i++)
		{
			ch[i] = (byte)(alphaNum.charAt(r.nextInt(alphaNum.length())));
		}
		try
		{
			str.append(new String(ch, "UTF-8") + "',");
		}
		catch(UnsupportedEncodingException e)
		{
			log("Exception: " + e.toString());
		}
		return str.toString();
	}

	protected void killTopology()
    {
		try
		{
			Map conf = Utils.readStormConfig();
			Client stormClient = NimbusClient.getConfiguredClient(conf).getClient();
			stormClient.killTopology(this.topologyName);

            //client.delete().forPath(zkCounterPath);
		}
		catch(NotAliveException e)
		{
			log("NotAliveException: " + e.getMessage());
		}
		catch(TException e)
		{	
			log("TException: " + e.getMessage());	
		}
    }

	protected long getContextId()
	{
		return Thread.currentThread().getId();
	}

	protected void log(String msg)
	{
		LOG.info(getContextId() + " - " + msg);
	}
}

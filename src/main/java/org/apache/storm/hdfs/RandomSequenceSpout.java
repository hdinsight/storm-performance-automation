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
import java.io.File;
import java.io.BufferedWriter;
import java.lang.StringBuilder;
import java.util.Random;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift7.TException;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class RandomSequenceSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;
	SpoutOutputCollector _collector;
	long maxCount;
	static AtomicInteger totalSpoutThreadsComplete = new AtomicInteger();
	boolean currentThreadMarkedComplete = false;
	int spoutParallelism = 0;
	long currCount = 0;
	long seqId = 0;
	long length;
	Random r = new Random();
	String alphaNum = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	List<Object> currentTuple;
	String topologyName;
	long startTime;
	File resultsFile;

	public RandomSequenceSpout(int sizeInBytes, int maxCount, String topologyName, int spoutParallelism) {
		this.topologyName = topologyName;
		this.spoutParallelism = spoutParallelism;
		this.maxCount = maxCount;
		this.startTime = System.currentTimeMillis();
		this.length = sizeInBytes;
		this.resultsFile = new File("/tmp/" + topologyName + ".txt");
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
		_collector.emit(this.currentTuple, id);
	}

	public void nextTuple() {
		if(currCount < maxCount)
		{
			Object id = Long.toString(seqId);
			currentTuple = new Values(getRandomSeqValue());
			_collector.emit(currentTuple, id);
			currCount++;
			seqId++;

			if (currCount % (maxCount/10) == 0)
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
				    bw.write(timeStamp + "," + currCount + "," + Long.toString(elapsedTime) + "\n"); 
				    bw.close();
				}
				catch(IOException ioe)
				{
				    System.err.println("IOException: " + ioe.getMessage());
				}
			}
		}
		else
		{

			try 
			{
    			Thread.sleep(30000);
			} catch(InterruptedException ex) 
			{
    			Thread.currentThread().interrupt();
			}

			try
			{
				Map conf = Utils.readStormConfig();
				Client client = NimbusClient.getConfiguredClient(conf).getClient();
				client.killTopology(topologyName);
			}
			catch(NotAliveException e)
			{

			}
			catch(TException e)
			{
				
			}

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
			System.out.println("Exception: "+e.toString());
		}
		return str.toString();
	}
}

package org.apache.storm.hdfs;

import org.apache.storm.eventhubs.spout.EventHubSpout;
import org.apache.storm.eventhubs.spout.EventHubSpoutConfig;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

public class PerfEventHubSpout extends EventHubSpout{
        private static final Logger logger = LoggerFactory.getLogger(PerfEventHubSpout.class);
        long ackmessages = 0;
        Map<Long,Long> ackstats=null;
        long maxCount;
        long currCount = 0;
        long startTime;
        long spoutThreads = 0;
        String zookeeperConnString;
        String topologyName;
        boolean spoutThreadComplete = false;
        public static ZookeeperClient zkClient;

    public PerfEventHubSpout(EventHubSpoutConfig spoutConfig, int maxCount, String zkConnStr,
                             int spoutThreads,String topologyName){
        super(spoutConfig,null,null,null);
        this.maxCount = maxCount;
        this.startTime = System.currentTimeMillis();
        this.zookeeperConnString = zkConnStr;
        this.spoutThreads = spoutThreads;
        this.topologyName = topologyName;
    }

    @Override
    public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
        super.open(config,context,collector);
    }

    @Override
    public void nextTuple() {
        if(this.currCount <= maxCount ) {
            super.nextTuple();
            this.currCount++;
        }
        else
        {
            if (!spoutThreadComplete)
            {
                this.zkClient = new ZookeeperClient(zookeeperConnString);
                // Attempt to increment shared counter in Zookeeper
                logger.info("Attempting to update shared counter in Zookeeper.");
                if(this.zkClient.updatedSharedWorkProgressCounter())
                {
                    spoutThreadComplete = true;
                    logger.info("Marking thread as complete.");
                }
            }
            else
            {
                long completedThreads = this.zkClient.getSharedCounterValue();
                logger.info("Checking if all other threads have completed work.");
                logger.info("Completed Threads: " + completedThreads + ", Spout Threads: " + this.spoutThreads);
                if (completedThreads == this.spoutThreads)
                {
                    logger.info("Killing topology");
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
                logger.info("Sleeping for 10 seconds..");
                Thread.sleep(10000);
            }
            catch (InterruptedException ex)
            {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void ack(Object msgId) {
        this.ackmessages++;
        if(this.ackstats == null){
            this.ackstats = new HashMap<Long, Long>();
        }
        if(this.ackmessages % (maxCount/10)==0){
            this.ackstats.put(this.ackmessages,System.currentTimeMillis() - this.startTime);
        }

        if(this.ackmessages == maxCount){
            if(this.zkClient == null){
                this.zkClient = new ZookeeperClient(zookeeperConnString);
            }
            ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
            try {
                ObjectOutputStream out = new ObjectOutputStream(byteOut);
                out.writeObject(this.ackstats);
            }catch (Exception e){
                logger.info("Exception encountered during serialization" + e.getMessage());
                throw new RuntimeException(e.getMessage());
            }

            this.zkClient.updatespoutstatistics(byteOut.toByteArray(),"/"+Thread.currentThread().getId());
        }
        super.ack(msgId);
    }

    @Override
    public void fail(Object msgId) {
        super.fail(msgId);
    }

    @Override
    public void deactivate() {
        super.deactivate();
    }

    @Override
    public void close() {
        super.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        super.declareOutputFields(declarer);
    }

    protected void killTopology()
    {
        try
        {
            Map conf = Utils.readStormConfig();
            Nimbus.Client stormClient = NimbusClient.getConfiguredClient(conf).getClient();
            stormClient.killTopology(this.topologyName);

            //client.delete().forPath(zkCounterPath);
        }
        catch(NotAliveException e)
        {
            logger.warn("NotAliveException: " + e.getMessage());
        }
        catch(Exception e)
        {
            logger.warn("TException: " + e.getMessage());
        }
    }
}

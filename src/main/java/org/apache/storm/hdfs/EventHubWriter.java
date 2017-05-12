package org.apache.storm.hdfs;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.eventhubs.bolt.EventHubBolt;
import org.apache.storm.eventhubs.bolt.EventHubBoltConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventHubWriter {
    private static final Logger LOG = LoggerFactory.getLogger(EventHubWriter.class);

    //Input Parameters
    @Parameter(names={"-workers","-w"}, description="Number of Worker Processes")
    static Integer workers = 0;

    @Parameter(names={"-recordSize","-x"}, description="Size of record being written in bytes.")
    static Integer recordSize = 0;

    @Parameter(names={"-spoutParallelism","-s"}, description="Number of spout instances across all worker processes.")
    static Integer spoutParallelism = 0;

    @Parameter(names={"-boltParallelism","-b"}, description="Number of bolt instances across all worker processes.")
    static Integer boltParallelism = 0;

    @Parameter(names={"-numRecords","-n"}, description="Number of records written by each instance of the bolt.")
    static Integer numRecords = 0;

    @Parameter(names={"-maxSpoutPending","-p"}, description="Max number of records alive in the topology that have not yet been acked.")
    static Integer maxSpoutPending = 0;

    @Parameter(names={"-topologyName","-y"}, description="Name of the topology name.")
    static String topologyName = "Storm_Perf_Toplogy";

    @Parameter(names={"-numTasksBolt","-t"}, description="Number of tasks executed by each bolt thread.")
    static Integer numTasksBolt = 0;

    @Parameter(names={"-numTasksSpout","-e"}, description="Number of tasks executed by each spout thread.")
    static Integer numTasksSpout = 0;

    @Parameter(names={"-numAckers","-k"}, description="Number of ackers.")
    static Integer numAckers = 0;

    @Parameter(names={"-eventhubs.writerpolicyname","-ew"}, description="EventHub Writer Policy name.")
    static String policyName = "dummy";

    @Parameter(names={"-eventhubs.writerpolicykey","-ewk"}, description="EventHub Writer Policy Key")
    static String policyKey = "dummy";

    @Parameter(names={"-eventhubs.namespace","-en"}, description="Namespace of the Eventhub")
    static String namespaceName = "dummy";

    @Parameter(names={"-eventhubs.entitypath","-ee"}, description="Eventhub Entitypath")
    static String entityPath = "dummy";

    public static void main(String[] args) throws Exception
    {
        // Parse Input parameters
        EventHubWriter eventHubWriterTopo = new EventHubWriter();
        JCommander cmdLineParser = new JCommander(eventHubWriterTopo,args);

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
        System.out.println("eventhubs.writerpolicyname " + policyName);
        System.out.println("eventhubs.writerpolicykey " + policyKey);
        System.out.println("eventhubs.namespace " + namespaceName);
        System.out.println("eventhubs.entitypath " + entityPath);

        // Getting Zookeeper connection string
        Configuration coresiteConfig = new Configuration();
        Path p = new Path("/etc/hadoop/conf/core-site.xml");
        coresiteConfig.addResource(p);
        String zkConnStr = coresiteConfig.get("ha.zookeeper.quorum");
        System.out.println("Zookeeper Hosts (ha.zookeeper.quorum): " + zkConnStr);

        // Build Topology
        TopologyBuilder builder = new TopologyBuilder();
        RandomSequenceSpout randomSeqSpout = new RandomSequenceSpout(recordSize, numRecords, topologyName, spoutParallelism, zkConnStr);

        builder.setSpout("testgenerator", randomSeqSpout, spoutParallelism)
                .setNumTasks(numTasksSpout);

        EventHubBoltConfig boltConfig = new EventHubBoltConfig(policyName, policyKey,
                namespaceName, "servicebus.windows.net", entityPath);

        builder.setBolt("eventhubbolt", new EventHubBolt(boltConfig), boltParallelism)
                .setNumTasks(numTasksBolt).localOrShuffleGrouping("testgenerator");

        // Set configurations
        Config conf = new Config();
        conf.setNumWorkers(workers);
        conf.setNumAckers(numAckers);
        conf.setMaxSpoutPending(maxSpoutPending);
        conf.put(Config.WORKER_CHILDOPTS, "-Xmx2g");
        conf.setDebug(false);

        //Submit Topology
        StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
    }
}

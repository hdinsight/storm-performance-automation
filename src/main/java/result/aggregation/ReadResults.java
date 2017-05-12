package result.aggregation;

import clojure.lang.IFn;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.storm.hdfs.ZookeeperClient;

import java.util.Map;

public class ReadResults {

    public static void main(String[] args) throws Exception {
        Configuration coresiteConfig = new Configuration();
        Path p = new Path("/etc/hadoop/conf/core-site.xml");
        coresiteConfig.addResource(p);
        String zkConnStr = coresiteConfig.get("ha.zookeeper.quorum");
        ZookeeperClient zkclient = new ZookeeperClient(zkConnStr);
        System.out.print("Acked Tuples ");
        System.out.print(" Elapsed Time in millis");
        System.out.println("");
        for(int i=0; i<args.length; i++){
            System.out.println("Result from spoutid "+args[i]);
            Map<Long,Long> ackstats = zkclient.getdata(args[i]);
            for (Long stat : ackstats.keySet()){
                System.out.print(stat);
                System.out.print(" "+ackstats.get(stat));
                System.out.println("");
            }
        }
    }

}

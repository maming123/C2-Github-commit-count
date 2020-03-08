package stormapplied.githubcommits;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import stormapplied.githubcommits.topology.CommitFeedListener;
import stormapplied.githubcommits.topology.EmailCounter;
import stormapplied.githubcommits.topology.EmailExtractor;

import java.util.HashMap;
import java.util.Map;

public class LocalTopologyRunner {
  private static final int TEN_MINUTES = 600000;

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("commit-feed-listener", new CommitFeedListener());

    builder
        .setBolt("email-extractor", new EmailExtractor())
        .shuffleGrouping("commit-feed-listener");

    builder
        .setBolt("email-counter", new EmailCounter())
        .fieldsGrouping("email-extractor", new Fields("email"));

    //为拓扑定义配置
    Map conf = new HashMap();
    //为拓扑配置工作进程数
    conf.put(Config.TOPOLOGY_WORKERS, 4);

    StormTopology topology = builder.createTopology();


    //start 本地测试
    /*// 创建本地集群(new 这个类即可)
    conf.put(Config.TOPOLOGY_DEBUG, true);// 打开DEBUG模式
    LocalCluster cluster = new LocalCluster();

    cluster.submitTopology("github-commit-count-topology",
        conf,
        topology);
    //向本地集群提交拓扑
    Utils.sleep(TEN_MINUTES);
    //停止拓扑
    cluster.killTopology("github-commit-count-topology");
    //关闭集群
    cluster.shutdown();*/
    //end 本地测试

    //提交拓扑到生产
    StormSubmitter.submitTopology("github-commit-count-topology", conf, topology);
  }
}

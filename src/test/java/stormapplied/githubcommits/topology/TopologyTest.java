package stormapplied.githubcommits.topology;

import org.apache.storm.Config;
import org.apache.storm.ILocalCluster;
import org.apache.storm.Testing;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.testing.CompleteTopologyParam;
import org.apache.storm.testing.MkClusterParam;
import org.apache.storm.testing.MockedSources;
import org.apache.storm.testing.TestJob;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertTrue;

public class TopologyTest {
  @Test
  public void verifyProperValuesAreEmittedByEachBolt() {
    Config config = new Config();
    config.setDebug(true);

    MkClusterParam clusterParam = new MkClusterParam();
    clusterParam.setSupervisors(1);
    clusterParam.setDaemonConf(config);

    Testing.withSimulatedTimeLocalCluster(clusterParam, new TestJob() {
      @Override
      public void run(ILocalCluster cluster) throws TException, InterruptedException {
        MockedSources mockedSources = new MockedSources();
        mockedSources.addMockData("commit-feed-listener", new Values("12345 test@manning.com"));

        Config config = new Config();
        config.setDebug(true);

        CompleteTopologyParam topologyParam = new CompleteTopologyParam();
        topologyParam.setMockedSources(mockedSources);
        topologyParam.setStormConf(config);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("commit-feed-listener", new CommitFeedListener());

        builder
            .setBolt("email-extractor", new EmailExtractor())
            .shuffleGrouping("commit-feed-listener");

        builder
            .setBolt("email-counter", new EmailCounter())
            .fieldsGrouping("email-extractor", new Fields("email"));

        StormTopology topology = builder.createTopology();

        Map result = Testing.completeTopology(cluster, topology, topologyParam);
        assertTrue(Testing.multiseteq(new Values(new Values("12345 test@manning.com")), Testing.readTuples(result, "commit-feed-listener")));
        assertTrue(Testing.multiseteq(new Values(new Values("test@manning.com")), Testing.readTuples(result, "email-extractor")));
        assertTrue(Testing.multiseteq(new Values(), Testing.readTuples(result, "email-counter")));
      }
    });
  }
}
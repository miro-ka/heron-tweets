package org.warnup.heron.tweets;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.warnup.heron.tweets.bolt.CountBolt;
import org.warnup.heron.tweets.bolt.ParseTweetBolt;
import org.warnup.heron.tweets.bolt.ReportBolt;
import org.warnup.heron.tweets.dto.InputConfigDto;
import org.warnup.heron.tweets.internal.JsonConfigParser;
import org.warnup.heron.tweets.spout.TwitterSpout;


import java.util.Properties;


public class TweetsTopology {

    public static void main(String[] args) throws Exception {

        JsonConfigParser config = new JsonConfigParser();
        final InputConfigDto configData = config.read();

        if(configData == null){
            System.out.println("Config file (config.json) is missing!");
            return;
        }


        TopologyBuilder builder = new TopologyBuilder();

        // Load config properties
        Properties properties = new Properties();
        properties.load(TweetsTopology.class.getResourceAsStream("/config.properties"));
        final String key = properties.getProperty("twitter.key");
        final String secret = properties.getProperty("twitter.secret");
        final String token = properties.getProperty("twitter.token");
        final String tokensecret = properties.getProperty("twitter.tokensecret");


        final TwitterSpout tweetSpout = new TwitterSpout(key, secret, token, tokensecret);


        // attach the tweet spout to the topology - parallelism of 1
        builder.setSpout("tweet-spout", tweetSpout, 1);
        builder.setBolt("split-tweet-bolt", new ParseTweetBolt(), 4).shuffleGrouping("tweet-spout");
        builder.setBolt("count-bolt", new CountBolt(), 4).fieldsGrouping("split-tweet-bolt", new Fields("tweet_id"));
        builder.setBolt("report-bolt", new ReportBolt(), 1).globalGrouping("count-bolt");


        Config conf = new Config();
        conf.setDebug(false);

        conf.setMaxSpoutPending(1000);
        conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-XX:+HeapDumpOnOutOfMemoryError");
        conf.setComponentRam("tweet-spout", 512L * 1024 * 1024); //0.5GB
        conf.setComponentRam("split-tweet-bolt", 512L * 1024 * 1024); //0.5GB
        conf.setComponentRam("count-bolt", 512L * 1024 * 1024); //0.5GB
        conf.setComponentRam("report-bolt", 512L * 1024 * 1024); //0.5GB
        conf.setContainerDiskRequested(1024L * 1024 * 1024); //1GB
        conf.setContainerCpuRequested(1);
        conf.setNumWorkers(3);
        StormSubmitter.submitTopology("tweets-topology", conf, builder.createTopology());


        //LocalCluster cluster = new LocalCluster();
        //cluster.submitTopology("warnup-grid", conf, builder.createTopology());
        //Utils.sleep(10000);
        //cluster.killTopology("warnup-grid");
        //cluster.shutdown();

    }
}

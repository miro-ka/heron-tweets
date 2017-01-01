package org.warnup.heron.tweets.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


import java.util.Map;

/**
 * A bolt that parses the tweet into words
 */
public class ParseTweetBolt extends BaseRichBolt
{
    // To output tuples from this bolt to the count bolt
    OutputCollector collector;

    @Override
    public void prepare(
            Map map,
            TopologyContext         topologyContext,
            OutputCollector         outputCollector)
    {
        // save the output collector for emitting tuples
        collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple)
    {
        // get the 1st column 'tweet' from tuple
        String[] tweets_array = tuple.getStringByField("tweet").split("\\|");
        long tweet_id = Long.parseLong(tweets_array[0]);
        int followers_count = Integer.parseInt(tweets_array[1]);
        collector.emit(new Values(tweet_id, followers_count));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        // tell storm the schema of the output tuple for this spout
        // tuple consists of a single column called 'tweet-word'
        declarer.declare(new Fields("tweet_id", "followers_count"));
    }
}

package org.warnup.heron.tweets.bolt;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import com.google.common.collect.Ordering;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;


public class CountBolt extends BaseRichBolt
{
    // To output tuples from this bolt to the next stage bolts, if any
    private OutputCollector collector;

    private Map<Long, Integer> tweetMap;
    final int topTweetsCount = 10;
    private long timer;
    final long saveInterval = 3600000; //every hour
    final int minCountValue = 10;

    @Override
    public void prepare(
            Map                     map,
            TopologyContext         topologyContext,
            OutputCollector         outputCollector)
    {
        // save the collector for emitting tuples
        collector = outputCollector;
        // create and initialize the map
        tweetMap = new ConcurrentHashMap<>();
        timer = System.currentTimeMillis();
    }


    private void emit_tweets(){
        final List<Integer> top_tweets_list = Ordering.natural().greatestOf(tweetMap.values(), topTweetsCount);
        final int min_list_count_value = (Collections.min(top_tweets_list));

        System.out.println("Emitting save for total of tweets: " +  String.valueOf(tweetMap.size()));

        for (Map.Entry<Long, Integer> entry : tweetMap.entrySet()) {
            Long tweet_id = entry.getKey();
            Integer value = entry.getValue();

            if(value > minCountValue && value >= min_list_count_value){
                collector.emit(new Values(tweet_id, value));
            }
        }

        tweetMap = new ConcurrentHashMap<>();
        //tweetMap.clear();
        System.out.println("Emit save done." );

    }

    @Override
    public void execute(Tuple tuple)
    {
        // get the id from the 1st column of incoming tuple
        Long tweet_id = tuple.getLong(0);
        // todo: we might need to use also favourites_count, to eliminate spam tweets


        // check if the word is present in the map
        Integer value = 1;
        if (tweetMap.get(tweet_id) == null){
            tweetMap.put(tweet_id, value);
        }
        else {
            value = tweetMap.get(tweet_id);
            tweetMap.put(tweet_id, ++value);
        }

        final long current_time = System.currentTimeMillis();

        if(current_time >= timer + saveInterval){
            emit_tweets();
            timer = current_time;
        }

        //collector.emit(new Values(tweet_id, value));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
    {
        outputFieldsDeclarer.declare(new Fields("tweet_id","count"));
    }
}


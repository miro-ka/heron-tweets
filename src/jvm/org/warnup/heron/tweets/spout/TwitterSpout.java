package org.warnup.heron.tweets.spout;


import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;


public class TwitterSpout extends BaseRichSpout {

    public static Logger log = LoggerFactory.getLogger(TwitterSpout.class);
    String key, secret, token, tokensecret;
    TwitterStream twitterStream;
    LinkedBlockingQueue<String> queue = null;
    SpoutOutputCollector collector;


    // Class for listening on the tweet stream - for twitter4j
    private class TweetListener implements StatusListener {

        // Callback function when a tweet arrives
        @Override public void onStatus(Status status) {

            //get the original tweet id
            Status original_tweet = status.getRetweetedStatus();
            long tweet_id;
            int followers_count;

            if(original_tweet != null){
                tweet_id = original_tweet.getId();
                followers_count = original_tweet.getUser().getFollowersCount();
            }
            else{
                tweet_id = status.getId();
                followers_count = status.getUser().getFollowersCount();
            }

            /*
            int retweet_count = status.getRetweetCount();
            int favourites_count = status.getFavoriteCount();
            long user_id = status.getUser().getId();
            String user_name = status.getUser().getName();
            String tweet_text = status.getText();
             */

            //convert numeric values to string
            Vector v = new Vector();
            v.addElement(Long.toString(tweet_id));
            v.addElement(Integer.toString(followers_count));
            /*
            v.addElement(Integer.toString(favourites_count));
            v.addElement(Integer.toString(followers));
            v.addElement(Long.toString(user_id));
            v.addElement(user_name);
            v.addElement(tweet_text);
            */

            Iterator itr = v.iterator();

            String delimString = "|";
            String out_string = "";
            while(itr.hasNext())
                out_string += itr.next() + delimString;

            queue.offer(out_string);

            /*
            String url;
            if(status.getURLEntities().length != 0){
                url = status.getURLEntities()[0].getURL();
                queue.offer(url);
            }*/

        }

        @Override public void onException(Exception e) {
            e.printStackTrace();
        }

        @Override public void onDeletionNotice(StatusDeletionNotice sdn) {}
        @Override public void onTrackLimitationNotice(int i) {}
        @Override public void onScrubGeo(long l, long l1) {}
        @Override public void onStallWarning(StallWarning warning) { }

    };


    public TwitterSpout(final String key, final String secret, final String token, final String tokensecret) {
        this.key = key;
        this.secret = secret;
        this.token = token;
        this.tokensecret = tokensecret;

    }


    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        // create the buffer to block tweets
        queue = new LinkedBlockingQueue<String>(1000);
        this.collector = collector;

        final ConfigurationBuilder config = new ConfigurationBuilder()
                .setOAuthConsumerKey(this.key)
                .setOAuthConsumerSecret(this.secret)
                .setOAuthAccessToken(this.token)
                .setOAuthAccessTokenSecret(this.tokensecret);



        TwitterStreamFactory fact = new TwitterStreamFactory(config.build());
        twitterStream = fact.getInstance();

        //add hashtag listener
        String keywords[] = {"news", "breaking"};
        FilterQuery tweetFilterQuery = new FilterQuery();
        tweetFilterQuery.language(new String[]{"en"});
        tweetFilterQuery.track(keywords);
        //tweetFilterQuery.follow

        twitterStream.addListener(new TweetListener());
        twitterStream.filter(tweetFilterQuery);


    }


    @Override
    public void close() {
        // shutdown the stream - when we are going to exit
        twitterStream.shutdown();
    }


    @Override
    public void nextTuple() {
        // try to pick a tweet from the buffer
        String ret = queue.poll();

        // if no tweet is available, wait for 50 ms and return
        if (ret==null) {
            Utils.sleep(50);
            return;
        }

        // now emit the tweet to next stage bolt
        collector.emit(new Values(ret));
    }


    @Override
    public void ack(Object msgId) {
    }


    @Override
    public void fail(Object msgId) {
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // tell storm the schema of the output tuple for this spout
        // tuple consists of a single column called 'tweet'
        declarer.declare(new Fields("tweet"));
    }


    @Override
    public Map<String, Object> getComponentConfiguration() {
        HashMap ret = new HashMap();
        ret.put("topology.max.task.parallelism", Integer.valueOf(1));
        return ret;
    }


}

package com.github.ckasera.kafka.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    TwitterProducer(){

    }
    public static void main(String[] args) {
        new TwitterProducer().run();


    }
    public void run(){
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
      Client client = createTwitterClient(msgQueue);
      client.connect();

        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            // something(msg);
            //profit();
            System.out.println(msg);
        }

        client.stop();
    }
    String consumerKey="EDZS1iOTSJD0TzO7bexIu5Y7f";
    String consumerSecret="c3CyLrY2UMiav7cXZWNa1vWpbRe5ns03yGhs1PmWMmgzGPsYw4";
    String token ="1032409462064050176-ZOZAMC4Abfpkcu6cXiVhIFQnt3Rn7y";
    String secret="xpMChMTwoYalbmABkI7ilLB0nAb20u9qQ9T2ix2WT48Pi";
    public Client createTwitterClient(BlockingQueue<String> msgQueue){
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        List<String> terms = Lists.newArrayList("Trump");

        hosebirdEndpoint.trackTerms(terms);
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret,
                token, secret);
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
                                        // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        return hosebirdClient;
// Attempts to establish a connection.
      //  hosebirdClient.connect();
    }
}

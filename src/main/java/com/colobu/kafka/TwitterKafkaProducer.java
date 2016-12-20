package com.colobu.kafka;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;


public class TwitterKafkaProducer {

        private static final String topic = "topic_master";

        private static void run() throws InterruptedException {

            System.out.println("arranca2");
            Properties properties = new Properties();
            properties.put("metadata.broker.list", "localhost:9092");
            properties.put("serializer.class", "kafka.serializer.StringEncoder");
            properties.put("client.id","niksisley");
            System.out.println("arranca3");
            ProducerConfig producerConfig = new ProducerConfig(properties);
            System.out.println("arranca4");
            Producer<String, String> producer = new Producer<String, String>(producerConfig);

            System.out.println("arranca5");
            BlockingQueue<String> queue = new LinkedBlockingQueue<String>(100000);
            StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
            System.out.println("arranca5");
            endpoint.trackTerms(Lists.newArrayList("twitterapi",
                    "#Futbol"));
            System.out.println("arranca5");

            String consumerKey=TwitterSourceConstant.CONSUMER_KEY_KEY;
            String consumerSecret=TwitterSourceConstant.CONSUMER_SECRET_KEY;
            String accessToken=TwitterSourceConstant.ACCESS_TOKEN_KEY;
            String accessTokenSecret=TwitterSourceConstant.ACCESS_TOKEN_SECRET_KEY;
            System.out.println("LLega");
            Authentication auth = new OAuth1(consumerKey, consumerSecret, accessToken,
                    accessTokenSecret);

            Client client = new ClientBuilder().hosts(Constants.STREAM_HOST)
                    .endpoint(endpoint).authentication(auth)
                    .processor(new StringDelimitedProcessor(queue)).build();

            client.connect();

            for (int msgRead = 0; msgRead < 1000; msgRead++) {
                KeyedMessage<String, String> message = null;
                try {
                    message = new KeyedMessage<String, String>(topic, queue.take());
                } catch (InterruptedException e) {
                    //e.printStackTrace();
                    System.out.println("Stream ended");
                }
                producer.send(message);
            }
            producer.close();
            client.stop();

        }

        public static void main(String[] args) {
            try {
                System.out.println("arranca");
                TwitterKafkaProducer.run();
            } catch (InterruptedException e) {
                System.out.println(e);
            }
        }
}


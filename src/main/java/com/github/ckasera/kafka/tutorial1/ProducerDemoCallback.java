package com.github.ckasera.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoCallback {
    public static void main(String[] args) {
        // System.out.println("Hello World !!");
        //create produce properties
        final Logger logger = LoggerFactory.getLogger(ProducerDemoCallback.class);
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());


        // create producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        for(int i=0;i<20;i++) {
            String key = "key_"+Integer.toString(i%5);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic",key, "Hello World Callback "+Integer.toString(i));

            //send message
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        recordMetadata.topic();
                        logger.info("Sending to Topic " + recordMetadata.topic() + "\n" +
                                "offset " + recordMetadata.offset() + "\n" +
                                "Partition " + recordMetadata.partition());
                    } else {
                        logger.error("Error while sending");
                    }
                }
            });
        }
        producer.close();
    }
}

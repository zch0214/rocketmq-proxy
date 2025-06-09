package com.example.demo;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

import java.util.Date;

public class CustomProducer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("ProducerGroupName");
        producer.setNamesrvAddr("localhost:9999");
        producer.start();

//        for (int i = 0; i < 5; i++){
            Message msg = new Message(
                    "test-topic",  // Topic
                    "Tag1",    // Tag
                    "YourKey",        // Key
                    new String(new Date().toString()).getBytes() // Body
            );
            producer.send(msg);
//        }

        producer.shutdown();
    }
}
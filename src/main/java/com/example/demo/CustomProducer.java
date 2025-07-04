package com.example.demo;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

import java.util.Date;

public class CustomProducer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("ProducerGroupName");
        producer.setNamesrvAddr("localhost:9999");
        producer.start();

        String dateStr = new Date().toString();
        Message msg = new Message(
                "test-topic",  // Topic
                "Tag1",    // Tag
                "YourKey",        // Key
                dateStr.getBytes() // Body
        );
        msg.putUserProperty("forTest", "ahaha");
        msg.putUserProperty("grayNode", "fals");
        System.out.println("send message: " + dateStr);
        producer.send(msg);

        producer.shutdown();
    }
}
package com.example.demo;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

import java.util.Date;

public class GrayProducer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("ProducerGroupName");
//        producer.setNamesrvAddr("localhost:9876");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();

        Message msg = new Message(
                "test-topic",  // Topic
                "Tag1",    // Tag
                "original_gray3",        // Key
                new String(new Date().toString()).getBytes() // Body
        );

        msg.putUserProperty("gray", "true");
        System.out.println("send message："+msg);
        producer.send(msg);

        producer.shutdown();
    }
}
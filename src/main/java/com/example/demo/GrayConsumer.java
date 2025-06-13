package com.example.demo;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

public class GrayConsumer {
    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("grayConsumer");
        consumer.setNamesrvAddr("localhost:9999");

        // 订阅Topic和Tag(*表示所有Tag)
//        consumer.subscribe("test-topic", "Tag1");
        consumer.subscribe("test-topic", MessageSelector.bySql("gray IS NOT NULL AND gray='true'"));
        // 注册回调处理消息
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                System.out.printf("收到消息: Topic=%s, Tag=%s, Key=%s, Body=%s %n",
                        msg.getTopic(),
                        msg.getTags(),
                        msg.getKeys(),
                        new String(msg.getBody()));
                System.out.println(msg.getUserProperty("gray"));
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        consumer.start();
        System.out.println("消费者已启动...");
    }
}

package com.example.demo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.filter.FilterAPI;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Description TODO
 * @Author zhouchuhang
 * @Date 2025/6/9 13:37
 **/
public class EnhanceUtil {

    public static Boolean NEED_TO_FILTER_GRAY = true;

    public static RemotingCommand enhanceSendMessageRequest(RemotingCommand original) {
        if (!NEED_TO_FILTER_GRAY) {
            return original;
        }
        String properties = original.getExtFields().get("i");
        HashMap<String, String> propertiesMap = (HashMap<String, String>) MessageDecoder.string2messageProperties(properties);
        if (!propertiesMap.get("grayNode").equals("true")) {
            return original;
        }
        propertiesMap.put("gray", "true");
        propertiesMap.put("blue", "oj8k");
        String newProperties = MessageDecoder.messageProperties2String(propertiesMap);

        original.getExtFields().put("i", newProperties);

//        original.setExtFields(propertiesMap);
//        System.out.println("[PROXY] Added gray mark. New properties: " + newProperties);
        return original;
    }
    public static RemotingCommand filterPullMessageRequest(RemotingCommand command) {
        if (!NEED_TO_FILTER_GRAY) {
            return command;
        }
        HeartbeatData  heartbeatData = HeartbeatData.decode(command.getBody(), HeartbeatData.class);

        for (ConsumerData consumerData : heartbeatData.getConsumerDataSet()) {
            for (SubscriptionData subscriptionData : consumerData.getSubscriptionDataSet()) {
                String expressionType = subscriptionData.getExpressionType();
                System.out.println("******修改前:"+subscriptionData);
                if (expressionType.contains(ExpressionType.SQL92)) {
                    String subString = subscriptionData.getSubString();
                    if (StringUtils.isBlank(subString)) {
                        subscriptionData.setSubString("gray IS NOT NULL AND gray = 'true'");
                    }else {
                        subscriptionData.setSubString(subString + " AND gray IS NOT NULL AND gray = 'true'");
                    }
                    System.out.println("******SQL92修改后:"+subscriptionData);
                }else{
                    SubscriptionDataUtil.convertTagToSqlSubscription(subscriptionData);
                    String subString = subscriptionData.getSubString();
                    subscriptionData.setSubString(subString + " AND gray IS NOT NULL AND gray = 'true'");
                    System.out.println("******TAG修改后:"+subscriptionData);
                }
            }
        }



        command.setBody(HeartbeatData.encode(heartbeatData));
        return command;
    }
}

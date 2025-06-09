package com.example.demo;

import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.HashMap;

/**
 * @Description TODO
 * @Author zhouchuhang
 * @Date 2025/6/9 13:37
 **/
public class EnhanceUtil {


    public static RemotingCommand enhanceSendMessageRequest(RemotingCommand original) {
        // 1. 克隆原始请求（避免污染原始对象）

        // 2. 获取原始properties（可能为null）
        String originProperties = original.getExtFields().get("properties");
        HashMap<String, String> propertiesMap = new HashMap<>();

        // 3. 解析原始properties（格式：key1=value1;key2=value2）
        if (originProperties != null && !originProperties.isEmpty()) {
            String[] kvs = originProperties.split(";");
            for (String kv : kvs) {
                String[] pair = kv.split("=", 2);
                if (pair.length == 2) {
                    propertiesMap.put(pair[0], pair[1]);
                }
            }
        }

        // 4. 添加灰度标记
        propertiesMap.put("gray", "true");

        // 5. 重新拼接properties字符串
        StringBuilder newProperties = new StringBuilder();
        propertiesMap.forEach((k, v) -> {
            if (newProperties.length() > 0) {
                newProperties.append(";");
            }
            newProperties.append(k).append("=").append(v);
        });

        // 6. 设置回请求中
        original.addExtField("properties", newProperties.toString());

        System.out.println("[PROXY] Added gray mark. New properties: " + newProperties);
        return original;
    }
    public static RemotingCommand filterPullMessageRequest(RemotingCommand pullRequest) {
        // 1. 获取消费者组的过滤条件（假设通过扩展字段传递）
        String consumerGroup = pullRequest.getExtFields().get("consumerGroup");
        String requiredGray = pullRequest.getExtFields().get("requireGray"); // 例如需要 gray=true

        // 2. 如果不需要过滤，直接返回原始请求
        if (requiredGray == null || !requiredGray.equals("true")) {
            return pullRequest;
        }

        // 3. 修改订阅表达式，添加对 gray=true 的过滤
        String subscription = pullRequest.getExtFields().get("subscription");
        if (subscription == null) {
            subscription = "";
        }

        // 4. 添加 SQL 过滤条件（假设消息的 user-property 中有 gray 字段）
        String newSubscription = subscription + " AND gray IS NOT NULL AND gray = 'true'";
        pullRequest.addExtField("subscription", newSubscription);
        System.out.println(pullRequest+"----");
        return pullRequest;
    }
}

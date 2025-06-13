package com.example.demo;

import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

/**
 * @Description TODO
 * @Author zhouchuhang
 * @Date 2025/6/10 14:52
 **/
public class SubscriptionDataUtil {
    public static SubscriptionData convertTagToSqlSubscription(SubscriptionData tagSub) {
        if (tagSub == null) {
            return null;
        }

        // 构建 SQL92 表达式
        String sqlExpression = convertTagFilterToSql(tagSub.getSubString());

        tagSub.setSubString(sqlExpression);
        tagSub.setExpressionType(ExpressionType.SQL92);

        return tagSub;
    }

    private static String convertTagFilterToSql(String tagFilter) {
        if (tagFilter == null || tagFilter.trim().isEmpty() || tagFilter.equals("*")) {
            return "TAGS IS NOT NULL";
        }

        String[] tags = tagFilter.split("\\|\\|");
        if (tags.length == 1) {
            return String.format("TAGS = '%s'", tags[0].trim());
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < tags.length; i++) {
            if (i > 0) {
                sb.append(" OR ");
            }
            sb.append(String.format("TAGS = '%s'", tags[i].trim()));
        }
        return sb.toString();
    }
}

package com.example.demo;

import  org.apache.rocketmq.common.protocol.RequestCode;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public class RequestCodeUtil {
    public static final Map<Integer, String> CODE_TO_NAME = new HashMap<>();

    static {
        // 初始化映射关系
        Field[] fields = RequestCode.class.getDeclaredFields();
        for (Field field : fields) {
            try {
                CODE_TO_NAME.put(field.getInt(null), field.getName());
            } catch (IllegalAccessException ignored) {
                // 忽略访问异常
            }
        }
    }
    public static String getCodeName(int code) {
        return CODE_TO_NAME.get(code);
    }

    public static void main(String[] args) {

        RequestCodeUtil.CODE_TO_NAME.forEach((code, name) -> {
            System.out.println("code:"+code+" name:"+name);
        });
    }
}

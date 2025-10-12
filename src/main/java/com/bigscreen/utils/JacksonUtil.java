package com.bigscreen.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;


/**
 * @author Xu huaiang
 * @date 2024/08/05
 * @description Jackson工具类
 */
public class JacksonUtil {
    public static final ObjectMapper INSTANCE;
    // 日期格式化
    private static final String STANDARD_FORMAT = "yyyy-MM-dd HH:mm:ss";

    static {
        INSTANCE = new ObjectMapper();
        //只返回非空字段
        INSTANCE.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        SimpleModule simpleModule = new SimpleModule(); // 创建简单模型
        // Long类型转String类型
        simpleModule.addSerializer(Long.class, ToStringSerializer.instance);
        INSTANCE.registerModule(simpleModule);
        //取消默认转换timestamps形式
        INSTANCE.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        //忽略空Bean转json的错误
        INSTANCE.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        //所有的日期格式都统一为以下的样式，即yyyy-MM-dd HH:mm:ss
        INSTANCE.setDateFormat(new SimpleDateFormat(STANDARD_FORMAT));
        // 设置时区
        INSTANCE.setTimeZone(TimeZone.getTimeZone("UTF-8"));
        //忽略 在json字符串中存在，但是在java对象中不存在对应属性的情况。防止错误
        INSTANCE.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    // 静态工厂方法获取单例
    public static ObjectMapper getInstance() {
        return INSTANCE;
    }

    /**
     * 设置jackson的命名策略
     * @param strategy
     */
    public static void setNamingStrategy(PropertyNamingStrategy strategy) {
        INSTANCE.setPropertyNamingStrategy(strategy);
    }

    private JacksonUtil() {
    }

    public static <T> T toObject(Object obj, Class<T> cls) {
        try {
            if (obj instanceof String) {
                return INSTANCE.readValue((String) obj, cls);
            }
            String str = toJson(obj);
            return INSTANCE.readValue(str, cls);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
    }

    public static <T> T toObject(Object obj, TypeReference<T> valueTypeRef) {
        try {
            if (obj instanceof String) {
                return INSTANCE.readValue((String) obj, valueTypeRef);
            }
            String str = toJson(obj);
            return INSTANCE.readValue(str, valueTypeRef);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
    }

    public static Map toMap(Object obj) {
        try {
            if (obj instanceof String) {
                return INSTANCE.readValue((String) obj, Map.class);
            }
            String str = toJson(obj);
            return INSTANCE.readValue(str, Map.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
    }

    public static String toJson(Object obj) {
        try {
            return INSTANCE.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
    }

    public static <T> List<T> toObjectList(Object obj, TypeReference<List<T>> typeReference) {
        try {
            if (obj instanceof String) {
                return INSTANCE.readValue((String) obj, typeReference);
            }
            String string = toJson(obj);
            return INSTANCE.readValue(string, typeReference);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
    }

    public static byte[] toBytes(Object obj) {
        try {
            return INSTANCE.writeValueAsBytes(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
    }

    public static JsonNode toTree(Object obj) {
        try {
            if (obj instanceof String) {
                return INSTANCE.readTree((String) obj);
            }
            String string = toJson(obj);
            return INSTANCE.readTree(string);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
    }
}

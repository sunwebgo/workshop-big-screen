package com.bigscreen.flink;

import cn.hutool.extra.spring.SpringUtil;
import com.bigscreen.entity.BinlogChangeInfo;
import com.bigscreen.utils.JacksonUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.springframework.stereotype.Component;

import java.util.Optional;


/**
 * @author Xu huaiang
 * @date 2024/08/04
 * @description 数据变更sink，用于处理数据变更
 */
@Slf4j
@Component
public class DataChangeSink implements SinkFunction<BinlogChangeInfo> {

    /**
     * @param binlogChangeInfo
     * @param context
     * @throws JsonProcessingException
     * @description SinkFunction接口及其实现类中不能够存在不可序列化的字段，
     * 否者会报错：The object probably contains or references non serializable fields.
     * 使用Spring Boot时，通过依赖注入获取bean是非常方便的，但是在工具化的应用场景下，
     * 想要动态获取bean就变得非常困难，于是Hutool封装了Spring中Bean获取的工具类——SpringUtil
     */
    @Override
    public void invoke(BinlogChangeInfo binlogChangeInfo, Context context) {
        log.info("===============监听到数据变化==============:{}", binlogChangeInfo);



    }

}


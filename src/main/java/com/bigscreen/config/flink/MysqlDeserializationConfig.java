package com.bigscreen.config.flink;

import com.bigscreen.entity.BinlogChangeInfo;
import com.bigscreen.eunm.OperationTypeEnum;
import com.bigscreen.utils.JacksonUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;
import java.util.Optional;

/**
 * @author Xu huaiang
 * @date 2024/08/03
 * @description Mysql数据源数据反序列化
 */
@Slf4j
public class MysqlDeserializationConfig implements DebeziumDeserializationSchema<BinlogChangeInfo> {

    public static final String TS_MS = "ts_ms";
    public static final String BIN_FILE = "file";
    public static final String POS = "pos";
    public static final String BEFORE = "before";
    public static final String AFTER = "after";
    public static final String SOURCE = "source";

    private final ObjectMapper objectMapper = JacksonUtil.getInstance();

    /**
     * 反序列化数据,转为变更JSON对象
     *
     * @param sourceRecord sourceRecord
     * @param collector    collector
     */
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<BinlogChangeInfo> collector) {
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        String database = fields[1];
        String tableName = fields[2];
        Struct struct = (Struct) sourceRecord.value();
        final Struct source = struct.getStruct(SOURCE);
        BinlogChangeInfo binlogChangeInfo = new BinlogChangeInfo();
        // 获取操作类型 READ CREATE UPDATE DELETE TRUNCATE;
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toUpperCase();
        // 一般情况是无需关心其之前之后数据的,直接获取最新的数据即可,但这里为了演示,都进行输出
        binlogChangeInfo.setBeforeData(JacksonUtil.toJson(getJsonObject(struct, BEFORE)));
        binlogChangeInfo.setAfterData(JacksonUtil.toJson(getJsonObject(struct, AFTER)));
        if (OperationTypeEnum.UPDATE.getType().equals(type)) {
            binlogChangeInfo.setData(JacksonUtil.toJson(getJsonObject(struct, BEFORE)));
        } else {
            binlogChangeInfo.setData(JacksonUtil.toJson(getJsonObject(struct, AFTER)));
        }
        binlogChangeInfo.setOperatorType(type);
        binlogChangeInfo.setFileName(Optional.ofNullable(source.get(BIN_FILE)).map(Object::toString).orElse(""));
        binlogChangeInfo.setFilePos(
                Optional.ofNullable(source.get(POS))
                        .map(x -> Integer.parseInt(x.toString()))
                        .orElse(0)
        );
        binlogChangeInfo.setDatabase(database);
        binlogChangeInfo.setTableName(tableName);
        binlogChangeInfo.setOperatorTime(Optional.ofNullable(struct.get(TS_MS))
                .map(x -> Long.parseLong(x.toString())).orElseGet(System::currentTimeMillis));
        // 输出数据
        collector.collect(binlogChangeInfo);
    }

    /**
     * 从元素数据获取出变更之前或之后的数据
     *
     * @param value        value
     * @param fieldElement fieldElement
     * @return JsonNode
     */
    private JsonNode getJsonObject(Struct value, String fieldElement) {
        Struct element = value.getStruct(fieldElement);
        ObjectNode jsonObject = objectMapper.createObjectNode();
        if (element != null) {
            Schema afterSchema = element.schema();
            List<Field> fieldList = afterSchema.fields();
            for (Field field : fieldList) {
                Object afterValue = element.get(field);
                jsonObject.putPOJO(field.name(), afterValue);
            }
        }
        return jsonObject;
    }

    @Override
    public TypeInformation<BinlogChangeInfo> getProducedType() {
        return TypeInformation.of(BinlogChangeInfo.class);
    }
}

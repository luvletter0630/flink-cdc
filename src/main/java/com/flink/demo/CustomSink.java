package com.flink.demo;

import com.alibaba.fastjson2.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * A custom sink function for Flink that writes data to Kafka.
 * It processes the input data as JSON strings and extracts the necessary information for writing to Kafka.
 * The supported operations are insert, update, delete, and init data.
 */
@Slf4j
public class CustomSink extends RichSinkFunction<String> {
    public static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    @Override
    public void invoke(String value, Context context) throws Exception {
        // parse the input JSON string
        JSONObject mysqlBinLog = JSONObject.parseObject(value);

        // get the runtime parameters
        ParameterTool parameters = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        String topic = parameters.get("topic");
        String clusterIp = parameters.get("clusterIp");
        // get the operation type
        String op = mysqlBinLog.getString("op");
        // 指定日期时间格式

        if ("u".equals(op)) {
            // for update operations, extract the before and after data
            JSONObject djson = setJsonObj("before", mysqlBinLog, "D");
            log.info("update data before： " + djson.toJSONString());
            JSONObject ujson = setJsonObj("after", mysqlBinLog, "U");
            // write the updated data to Kafka
            KafkaProducer.sendMessage(clusterIp, topic, "mysqlupdate", ujson.toJSONString());
            log.info("update data after: " + ujson.toJSONString());
        } else if ("d".equals(op)) {
            // for delete operations, extract the before data
            JSONObject deleteData = setJsonObj("before", mysqlBinLog, "D");
            log.info("delete data: " + deleteData.toJSONString());
            // write the deleted data to Kafka
            KafkaProducer.sendMessage(clusterIp, topic, "mysqldelete", deleteData.toJSONString());
        } else if ("c".equals(op)) {
            // for insert operations, extract the after data
            JSONObject insertData = setJsonObj("after", mysqlBinLog, "I");
            log.info("insert data: " + insertData.toJSONString());
            // write the inserted data to Kafka
            KafkaProducer.sendMessage(clusterIp, topic, "mysqlinsert", insertData.toJSONString());
        } else if ("r".equals(op)) {
            // for init data operations, extract the after data
            JSONObject initData = setJsonObj("after", mysqlBinLog, "I");
            log.info("init data: " + initData.toJSONString());
            // write the init data to Kafka
            KafkaProducer.sendMessage(clusterIp, topic, "mysqlinitdata", initData.toJSONString());
        }

    }

    private static JSONObject setJsonObj(String status, JSONObject mysqlObject, String signature) {
        JSONObject json = mysqlObject.getJSONObject(status);
        json.put("DELETE_SIGN", signature);
        json.put("_DATA_CHANGE_TIME_CDC", LocalDateTime.now().format(dtf));
        return json;
    }
}
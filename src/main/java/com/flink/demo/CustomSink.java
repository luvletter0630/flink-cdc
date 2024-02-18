package com.flink.demo;

import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * A custom sink function for Flink that writes data to Kafka.
 * It processes the input data as JSON strings and extracts the necessary information for writing to Kafka.
 * The supported operations are insert, update, delete, and init data.
 */
public class CustomSink extends RichSinkFunction<String> {

    @Override
    public void invoke(String value, Context context) throws Exception {
        // parse the input JSON string
        JSONObject mysqlBinLog = JSONObject.parseObject(value);

        // get the runtime parameters
        ParameterTool parameters = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        String topic = parameters.get("topic");

        // get the operation type
        String op = mysqlBinLog.getString("op");

        if ("u".equals(op)) {
            // for update operations, extract the before and after data
            JSONObject djson = mysqlBinLog.getJSONObject("before");
            djson.put("flinkcdc_binlog_optype", "D");
            JSONObject ijson = mysqlBinLog.getJSONObject("after");
            ijson.put("flinkcdc_binlog_optype", "I");
            // write the updated data to Kafka
            KafkaProducer.sendMessage(topic, "mysqlupdate", ijson.toJSONString());
        } else if ("d".equals(op)) {
            // for delete operations, extract the before data
            JSONObject deleteData = mysqlBinLog.getJSONObject("before");
            deleteData.put("flinkcdc_binlog_optype", "D");
            // write the deleted data to Kafka
            KafkaProducer.sendMessage(topic, "mysqldelete", deleteData.toJSONString());
        } else if ("c".equals(op)) {
            // for insert operations, extract the after data
            JSONObject insertData = mysqlBinLog.getJSONObject("after");
            insertData.put("flinkcdc_binlog_optype", "I");
            // write the inserted data to Kafka
            KafkaProducer.sendMessage(topic, "mysqlinsert", insertData.toJSONString());
        } else if ("r".equals(op)) {
            // for init data operations, extract the after data
            JSONObject initData = mysqlBinLog.getJSONObject("after");
            initData.put("flinkcdc_binlog_optype", "U");
            // write the init data to Kafka
            KafkaProducer.sendMessage(topic, "mysqlinitdata", initData.toJSONString());
            System.out.println(initData.toJSONString());
        }
    }
}
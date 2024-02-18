package com.flink.demo;

import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class CustomSink extends RichSinkFunction<String> {
    @Override
    public void invoke(String value, Context context) throws Exception {
        JSONObject mysqlBinLog = JSONObject.parseObject(value);
        ParameterTool parameters = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        String topic = parameters.get("topic");
        String op = mysqlBinLog.getString("op");
        if ("u".equals(op)) {
            JSONObject djson = mysqlBinLog.getJSONObject("before");
            djson.put("flinkcdc_binlog_optype", "D");
            JSONObject ijson = mysqlBinLog.getJSONObject("after");
            ijson.put("flinkcdc_binlog_optype", "I");
            KafkaProducer.sendMessage(topic, "mysqlupdate", ijson.toJSONString());
        } else if ("d".equals(op)) {
            JSONObject deleteData = mysqlBinLog.getJSONObject("before");
            deleteData.put("flinkcdc_binlog_optype", "D");
            KafkaProducer.sendMessage(topic, "mysqldelete", deleteData.toJSONString());
        } else if ("c".equals(op)) {
            JSONObject insertData = mysqlBinLog.getJSONObject("after");
            insertData.put("flinkcdc_binlog_optype", "I");
            KafkaProducer.sendMessage(topic, "mysqlinsert", insertData.toJSONString());
        } else if ("r".equals(op)) {
            JSONObject initData = mysqlBinLog.getJSONObject("after");
            initData.put("flinkcdc_binlog_optype", "U");
            KafkaProducer.sendMessage(topic, "mysqlinitdata", initData.toJSONString());
            System.out.println(initData.toJSONString());
        }
    }
}
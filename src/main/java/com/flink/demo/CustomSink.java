package com.flink.demo;

import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONWriter;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class CustomSink extends RichSinkFunction<String> {
    @Override
    public void invoke(String value, Context context) throws Exception {
        JSONObject mysqlBinLog = JSONObject.parseObject(value);
        String op = mysqlBinLog.getString("op");
        if ("u".equals(op)){
            JSONObject djson = mysqlBinLog.getJSONObject("before");
            djson.put("flinkcdc_binlog_optype","D");
            System.out.println(djson.toJSONString(JSONWriter.Feature.WriteNulls));
            JSONObject ijson = mysqlBinLog.getJSONObject("after");
            ijson.put("flinkcdc_binlog_optype","I");
            System.out.println(ijson.toJSONString(JSONWriter.Feature.WriteNulls));
        }else if ("d".equals(op)){
            JSONObject deleteData = mysqlBinLog.getJSONObject("before");
            deleteData.put("flinkcdc_binlog_optype","D");
            System.out.println(deleteData.toJSONString(JSONWriter.Feature.WriteNulls));
        }else if ("c".equals(op)){
            JSONObject insertData = mysqlBinLog.getJSONObject("after");
            insertData.put("flinkcdc_binlog_optype","I");
            System.out.println(insertData.toJSONString(JSONWriter.Feature.WriteNulls));
        }else if ("r".equals(op)){
            JSONObject initData = mysqlBinLog.getJSONObject("after");
            initData.put("flinkcdc_binlog_optype","U");
            System.out.println(initData.toJSONString(JSONWriter.Feature.WriteNulls));
        }
    }
}
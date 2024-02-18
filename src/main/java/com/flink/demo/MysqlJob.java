package com.flink.demo;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @className: MysqlJob
 * @description: mysql
 * @author: liwj
 * @date: 2023/11/14
 **/
public class MysqlJob {

    public static void main(String[] args) throws Exception {
        MySqlSourceBuilder<String> builder = MySqlSource.builder();
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostname = parameterTool.get("hostname");
        int port = parameterTool.getInt("port");
        String databaseList = parameterTool.get("databaseList");
        String tableList = parameterTool.get("tableList");
        String username = parameterTool.get("username");
        String password = parameterTool.get("password");
        boolean initialized = parameterTool.getBoolean("initialized");
        MySqlSource<String> source = null;

        if (initialized) {
            source = builder.hostname(hostname)
                    .port(port)
                    .databaseList(databaseList)
                    .tableList(tableList)
                    .username(username)
                    .password(password)
                    .deserializer(new JsonDebeziumDeserializationSchema())
                    .includeSchemaChanges(true)
                    .startupOptions(StartupOptions.initial())
                    .build();
        } else {
            source = builder.hostname(hostname)
                    .port(port)
                    .databaseList(databaseList)
                    .tableList(tableList)
                    .username(username)
                    .password(password)
                    .deserializer(new JsonDebeziumDeserializationSchema())
                    .includeSchemaChanges(true)
                    .startupOptions(StartupOptions.latest())
                    .build();
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameterTool);
        env.enableCheckpointing(3000);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //配置mysql数据源
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "MYSQL Source")
                //配置输出，可以发送到kafka
                .addSink(new CustomSink());

        env.execute();
    }
}

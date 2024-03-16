package com.flink.demo;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.JobExecutionResult;
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

    /**
     * Main method for the MysqlJob.
     *
     * @param args command line arguments
     */
    public static void main(String[] args) throws Exception {
        MySqlSourceBuilder<String> builder = MySqlSource.builder();
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostname = parameterTool.get("hostname");
        int port = parameterTool.getInt("port");
        String databaseList = parameterTool.get("databaseList");
        String tableList = parameterTool.get("tableList");
        String username = parameterTool.get("username");
        String password = parameterTool.get("password");
        int opType = parameterTool.getInt("opType");

        MySqlSource<String> source = builder.hostname(hostname)
                .port(port)
                .databaseList(databaseList)
                .tableList(tableList)
                .username(username)
                .password(password)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .includeSchemaChanges(true)
                .startupOptions(opType == 2 ? StartupOptions.initial() : StartupOptions.latest())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameterTool);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // configure mysql source
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "MYSQL Source")
                // configure output, can send to kafka
                .addSink(new CustomSink()).setParallelism(1);

        JobExecutionResult jobExecutionResult = env.execute();
        jobExecutionResult.getJobID();
    }
}

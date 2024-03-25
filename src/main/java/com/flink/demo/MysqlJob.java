package com.flink.demo;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @className: MysqlJob
 * @description: mysql
 * @author: liwj
 * @date: 2023/11/14
 **/
@Slf4j
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
                .debeziumProperties(getDebeziumProperties())
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
        JobID jobID = env.execute().getJobExecutionResult().getJobID();
        log.info("job started successfully jodid = {}", jobID.toString());
    }

    private static Properties getDebeziumProperties() {
        Properties properties = new Properties();
        properties.setProperty("converters", "dateConverters");
        //根据类在那个包下面修改
        properties.setProperty("dateConverters.type", "com.flink.demo.MySqlDateTimeConverter");
        properties.setProperty("dateConverters.format.date", "yyyy-MM-dd");
        properties.setProperty("dateConverters.format.time", "HH:mm:ss");
        properties.setProperty("dateConverters.format.datetime", "yyyy-MM-dd HH:mm:ss");
        properties.setProperty("dateConverters.format.timestamp", "yyyy-MM-dd HH:mm:ss");
        properties.setProperty("dateConverters.format.timestamp.zone", "UTC+8");
        properties.setProperty("debezium.snapshot.locking.mode", "none"); //全局读写锁，可能会影响在线业务，跳过锁设置
        properties.setProperty("include.schema.changes", "true");
        properties.setProperty("bigint.unsigned.handling.mode", "long");
        properties.setProperty("decimal.handling.mode", "double");
        return properties;
    }
}

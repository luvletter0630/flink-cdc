package com.flink.demo;

import com.flink.demo.alarm.AlarmEntity;
import com.flink.demo.alarm.AlarmUtil;
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

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;

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
    public static void main(String[] args) {
        MySqlSourceBuilder<String> builder = MySqlSource.builder();
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostname = parameterTool.get("hostname");
        int port = parameterTool.getInt("port");
        String databaseList = parameterTool.get("databaseList");
        String username = parameterTool.get("username");
        String password = parameterTool.get("password");
        int opType = parameterTool.getInt("opType");
        int jobId = parameterTool.getInt("jobId");
        List<AlarmEntity> alarmconfig = getalarm(jobId);
        Map<String, Map<String, String>> tableconfig = getJobExt(jobId);
        StringBuilder tableList = new StringBuilder();
        tableconfig.keySet().forEach(key -> tableList.append(databaseList).append('.').append(key).append(','));
        MySqlSource<String> source = builder.hostname(hostname)
                .port(port)
                .databaseList(databaseList)
                .tableList(tableconfig.toString())
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
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        java.util.Date date = new java.util.Date();
        String formattedDate = formatter.format(date);
        try {
            JobID taskId = env.execute().getJobExecutionResult().getJobID();
            if (alarmconfig.size() > 0) {
                alarmconfig.forEach(e -> {
                    if ("1".equals(e.getAlertSms())) {
                        AlarmUtil.sendForImm(e.getAlarmPerson(), formattedDate + ":" + "flink任务启动成功任务ID： " + taskId + ", DataBase:" + databaseList + ", TableList:" + tableList, jobId);
                    }
                    if ("1".equals(e.getAlertImm())) {
                        AlarmUtil.sendForMessage(e.getAlarmPhone(), formattedDate + ":" + "flink任务启动成功任务ID： " + taskId + ", DataBase:" + databaseList + ", TableList:" + tableList, jobId);
                    }
                });
            }
        } catch (Exception e1) {
            if (alarmconfig.size() > 0) {
                alarmconfig.forEach(e -> {
                    if ("1".equals(e.getAlertSms())) {
                        AlarmUtil.sendForImm(e.getAlarmPerson(), formattedDate + ":" + "flink任务启动失败" + e1.getMessage() + ", DataBase:" + databaseList + ", TableList:" + tableList, jobId);
                    }
                    if ("1".equals(e.getAlertImm())) {
                        AlarmUtil.sendForMessage(e.getAlarmPhone(), formattedDate + ":" + "flink任务启动失败" + e1.getMessage() + ", DataBase:" + databaseList + ", TableList:" + tableList, jobId);
                    }
                });
            }
            throw new RuntimeException(e1);
        }
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

    private static List<AlarmEntity> getalarm(int jobId) {
        List<AlarmEntity> Alarms = new ArrayList<AlarmEntity>();
        try (Connection conn = DriverManager.getConnection("jdbc:mysql://172.30.142.231:3306/system_service_control_test?serverTimezone=UTC&useUnicode=true&characterEncoding=utf-8&useSSL=false", "manager", "Scal@YcaCn123")) {
            String sql = "SELECT alarm_phone,alarm_person, ALERT_SMS , ALERT_IMM FROM job_alarm_config WHERE job_id = ? and type = 1";
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setInt(1, jobId);
                try (ResultSet rs = pstmt.executeQuery();) {
                    while (rs.next()) {
                        AlarmEntity alarmuser = new AlarmEntity();
                        alarmuser.setAlarmPhone(rs.getString("alarm_phone"));
                        alarmuser.setAlarmPerson(rs.getString("alarm_person"));
                        alarmuser.setAlertImm(rs.getString("ALERT_IMM"));
                        alarmuser.setAlertSms(rs.getString("ALERT_SMS"));
                        Alarms.add(alarmuser);
                    }
                }
            }
            return Alarms;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return Alarms;
    }

    private static Map getJobExt(int jobId) {
        try (Connection conn = DriverManager.getConnection("jdbc:mysql://172.30.142.231:3306/system_service_control_test?serverTimezone=UTC&useUnicode=true&characterEncoding=utf-8&useSSL=false", "manager", "Scal@YcaCn123")) {
            String sql = "SELECT table_name, topic_name , isenc,enctype,ENC_COL,ispartitioncol,PARTITIONCOL FROM job_config_extend WHERE job_config_id = ?";
            Map<String, Map<String, String>> jobExtConfig = new HashMap<>();
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setInt(1, jobId);
                ResultSet rs = pstmt.executeQuery();
                while (rs.next()) {
                    Map<String, String> tableConfig = new HashMap<String, String>();
                    tableConfig.put("topic_name", rs.getString("topic_name"));
                    tableConfig.put("isenc", rs.getString("isenc"));
                    tableConfig.put("enctype", rs.getString("enctype"));
                    tableConfig.put("ENC_COL", rs.getString("ENC_COL"));
                    tableConfig.put("ispartitioncol", rs.getString("ispartitioncol"));
                    tableConfig.put("PARTITIONCOL", rs.getString("PARTITIONCOL"));
                    String[] tableName = rs.getString("table_name").split("\\.");
                    jobExtConfig.put(tableName[1], tableConfig);
                }
            }
            return jobExtConfig;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

}

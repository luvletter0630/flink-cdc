package com.flink.demo;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.flink.demo.alarm.AlarmEntity;
import com.flink.demo.alarm.AlarmUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.concurrent.ExecutionException;

/**
 * A custom sink function for Flink that writes data to Kafka.
 * It processes the input data as JSON strings and extracts the necessary information for writing to Kafka.
 * The supported operations are insert, update, delete, and init data.
 */
@Slf4j
public class CustomSink extends RichSinkFunction<String> {
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private Map<String, Map<String, String>> jobExtConfig;

    @Override
    public void invoke(String value, Context context) {
        JSONObject mysqlBinLog = JSONObject.parseObject(value);

        // get the runtime parameters
        ParameterTool parameters = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        String user = parameters.get("managerUser");
        String pwd = parameters.get("managerPwd");
        String databaseName = parameters.get("databaseList");
        String clusterIp = parameters.get("clusterIp");
        int taskId = parameters.getInt("jobId");
        // get the operation type
        String op = mysqlBinLog.getString("op");
        String jobId = getRuntimeContext().getJobId().toString();
        if (jobExtConfig == null) {
            List<AlarmEntity> alarmconfig = getalarm(taskId);
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            java.util.Date date = new java.util.Date();
            String formattedDate = formatter.format(date);
            if (alarmconfig.size() > 0) {
                alarmconfig.forEach(e -> {
                    if ("1".equals(e.getAlertSms())) {
                        AlarmUtil.sendForImm(e.getAlarmPerson(), formattedDate + ":" + "任务Id：" + taskId + "flink任务ID： " + jobId + ", DataBase:" + databaseName + ", 可能发生启动或重启", taskId);
                    }
                    if ("1".equals(e.getAlertImm())) {
                        AlarmUtil.sendForMessage(e.getAlarmPhone(), formattedDate + ":" + "任务Id：" + taskId + "flink任务ID： " + jobId + ", DataBase:" + databaseName + ", 可能发生启动或重启", taskId);
                    }
                });
            }
            jobExtConfig = getJobExt(taskId);
        }
        Map<String, String> tableConfig = jobExtConfig.get(mysqlBinLog.getJSONObject("source").getString("table"));
        if (tableConfig != null) {
            String topic = tableConfig.get("topic_name");
            String partitioncol = "0".equals(tableConfig.get("ispartitioncol")) ? tableConfig.get("PARTITIONCOL") : null;
            // 指定日期时间格式
            if ("u".equals(op)) {
                // for update operations, extract the before and after data
                JSONObject djson = setJsonObj("before", mysqlBinLog, "D");
                sendToKafka(topic, clusterIp, jobId, djson, user, pwd, partitioncol, taskId, databaseName, mysqlBinLog.getJSONObject("source").getString("table"));
                JSONObject ujson = setJsonObj("after", mysqlBinLog, "U");
                // write the updated data to Kafka
                sendToKafka(topic, clusterIp, jobId, ujson, user, pwd, partitioncol, taskId, databaseName, mysqlBinLog.getJSONObject("source").getString("table"));
            } else if ("d".equals(op)) {
                // for delete operations, extract the before data
                JSONObject deleteData = setJsonObj("before", mysqlBinLog, "D");
                sendToKafka(topic, clusterIp, jobId, deleteData, user, pwd, partitioncol, taskId, databaseName, mysqlBinLog.getJSONObject("source").getString("table"));
                // write the deleted data to Kafka
            } else if ("c".equals(op)) {
                // for insert operations, extract the after data
                JSONObject insertData = setJsonObj("after", mysqlBinLog, "I");
                sendToKafka(topic, clusterIp, jobId, insertData, user, pwd, partitioncol, taskId, databaseName, mysqlBinLog.getJSONObject("source").getString("table"));
            } else if ("r".equals(op)) {
                // for init data operations, extract the after data
                JSONObject initData = setJsonObj("after", mysqlBinLog, "I");
                sendToKafka(topic, clusterIp, jobId, initData, user, pwd, partitioncol, taskId, databaseName, mysqlBinLog.getJSONObject("source").getString("table"));
            }
        } else {
            log.info("restart taskId = {} table = {}", taskId, mysqlBinLog.getJSONObject("source").getString("table"));
            log.info("current map={}", jobExtConfig.toString());
            List<AlarmEntity> alarmconfig = getTroblealarm(taskId);
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            java.util.Date date = new java.util.Date();
            String formattedDate = formatter.format(date);
            if (alarmconfig.size() > 0) {
                alarmconfig.forEach(e -> {
                    if ("1".equals(e.getAlertSms())) {
                        AlarmUtil.sendForImm(e.getAlarmPerson(), formattedDate + ":" + "任务Id：" + taskId + "flink任务ID： " + jobId + ", DataBase:" + databaseName + "获取表" + mysqlBinLog.getJSONObject("source").getString("table") + "Topic失败", taskId);
                    }
                    if ("1".equals(e.getAlertImm())) {
                        AlarmUtil.sendForMessage(e.getAlarmPhone(), formattedDate + ":" + "任务Id：" + taskId + "flink任务ID： " + jobId + ", DataBase:" + databaseName + "获取表" + mysqlBinLog.getJSONObject("source").getString("table") + "Topic失败", taskId);
                    }
                });
            }
        }
    }

    private void sendToKafka(String topic, String clusterIp, String jobId, JSONObject jsonobj, String user, String pwd, String partitioncol, int taskId, String databaseName, String tableName) {
        try {
            String insertJsonStr = JSON.toJSONString(jsonobj);
            KafkaProducer.sendMessage(clusterIp, topic, partitioncol, insertJsonStr, user, pwd, taskId, databaseName, tableName);
        } catch (ExecutionException e1) {
            List<AlarmEntity> alarmconfig = getTroblealarm(taskId);
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            java.util.Date date = new java.util.Date();
            String formattedDate = formatter.format(date);
            if (alarmconfig.size() > 0) {
                alarmconfig.forEach(e -> {
                    if ("1".equals(e.getAlertSms())) {
                        AlarmUtil.sendForImm(e.getAlarmPerson(), formattedDate + ":" + "kafka推送消息失败flink任务Id：" + jobId + "任务ID： " + taskId + ", DataBase:" + databaseName + ", TableName:" + tableName + "send to Topic:" + topic + "异常信息" + e1.getMessage(), taskId);
                    }
                    if ("1".equals(e.getAlertImm())) {
                        AlarmUtil.sendForMessage(e.getAlarmPhone(), formattedDate + ":" + "kafka推送消息失败flink任务Id：" + jobId + "任务ID： " + taskId + ", DataBase:" + databaseName + ", TableName:" + tableName + "send to Topic:" + topic + "异常信息" + e1.getMessage(), taskId);
                    }
                });
            }
            throw new RuntimeException(e1);
        } catch (InterruptedException e1) {
            List<AlarmEntity> alarmconfig = getTroblealarm(taskId);
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            java.util.Date date = new java.util.Date();
            String formattedDate = formatter.format(date);
            if (alarmconfig.size() > 0) {
                alarmconfig.forEach(e -> {
                    if ("1".equals(e.getAlertSms())) {
                        AlarmUtil.sendForImm(e.getAlarmPerson(), formattedDate + ":" + "kafka推送消息失败flink任务Id：" + jobId + "任务ID： " + taskId + ", DataBase:" + databaseName + ", TableName:" + tableName + "send to Topic:" + topic + "异常信息" + e1.getMessage(), taskId);
                    }
                    if ("1".equals(e.getAlertImm())) {
                        AlarmUtil.sendForMessage(e.getAlarmPhone(), formattedDate + ":" + "kafka推送消息失败flink任务Id：" + jobId + "任务ID： " + taskId + ", DataBase:" + databaseName + ", TableName:" + tableName + "send to Topic:" + topic + "异常信息" + e1.getMessage(), taskId);
                    }
                });
            }
            throw new RuntimeException(e1);
        }
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

    private static List<AlarmEntity> getTroblealarm(int jobId) {
        List<AlarmEntity> Alarms = new ArrayList<AlarmEntity>();
        try (Connection conn = DriverManager.getConnection("jdbc:mysql://172.30.142.231:3306/system_service_control_test?serverTimezone=UTC&useUnicode=true&characterEncoding=utf-8&useSSL=false", "manager", "Scal@YcaCn123")) {
            String sql = "SELECT alarm_phone,alarm_person, ALERT_SMS , ALERT_IMM FROM job_alarm_config WHERE job_id = ? and type = 5";
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setInt(1, jobId);
                try (ResultSet rs = pstmt.executeQuery()) {
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

    private static JSONObject setJsonObj(String status, JSONObject mysqlObject, String signature) {
        JSONObject json = mysqlObject.getJSONObject(status);
        json.put("DELETE_SIGN", signature);
        // 将long类型时间戳转为Date对象
        json.put("_DATA_CHANGE_TIME_BINLOG", sdf.format(new Date(mysqlObject.getJSONObject("source").getLong("ts_ms"))));
        json.put("_DATA_CHANGE_TIME_CDC", sdf.format(new Date(mysqlObject.getLong("ts_ms"))));
        return json;
    }
}
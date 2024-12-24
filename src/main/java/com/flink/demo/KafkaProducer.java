package com.flink.demo;

import com.flink.demo.alarm.AlarmEntity;
import com.flink.demo.alarm.AlarmUtil;
import com.flink.demo.kafka.ProducerSingleton;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * This class provides methods for producing messages to Kafka topics.
 */
@Slf4j
public class KafkaProducer {
    /**
     * Sends a message to a Kafka topic.
     *
     * @param topic   the name of the Kafka topic
     * @param key     the key for the message
     * @param message the messag  to be sent
     */

    public static void sendMessage(String clusterIp, String topic, String key, String message, String user, String pwd, int taskId, String databaseName, String tableName) throws ExecutionException, InterruptedException {
        Producer<String, String> producer = ProducerSingleton.getProducer(clusterIp, user, pwd);
        // Send the message
        producer.send(new ProducerRecord<>(topic, key, message), (metadata, exception) -> {
            if (exception != null) {
                List<AlarmEntity> alarmconfig = getTroblealarm(taskId);
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                java.util.Date date = new java.util.Date();
                String formattedDate = formatter.format(date);
                if (alarmconfig.size() > 0) {
                    alarmconfig.forEach(e -> {
                        if ("1".equals(e.getAlertSms())) {
                            AlarmUtil.sendForImm(e.getAlarmPerson(), formattedDate + ":" + "kafka推送消息失败flink任务Id：" + taskId + ", DataBase:" + databaseName + ", TableName:" + tableName + "send to Topic:" + topic + "异常信息" + exception.getMessage(), taskId);
                        }
                        if ("1".equals(e.getAlertImm())) {
                            AlarmUtil.sendForMessage(e.getAlarmPhone(), formattedDate + ":" + "kafka推送消息失败flink任务Id：" + taskId + ", DataBase:" + databaseName + ", TableName:" + tableName + "send to Topic:" + topic + "异常信息" + exception.getMessage(), taskId);
                        }
                    });
                }
            }
        });
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
}

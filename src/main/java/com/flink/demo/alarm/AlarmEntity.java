package com.flink.demo.alarm;

/**
 * @author liwj
 * @date 2024/12/5 16:28
 * @description:
 */
public class AlarmEntity {
    private String alarmPhone;
    private String alarmPerson;
    private String alertSms;
    private String alertImm;

    public String getAlarmPhone() {
        return alarmPhone;
    }

    public void setAlarmPhone(String alarmPhone) {
        this.alarmPhone = alarmPhone;
    }

    public String getAlarmPerson() {
        return alarmPerson;
    }

    public void setAlarmPerson(String alarmPerson) {
        this.alarmPerson = alarmPerson;
    }

    public String getAlertSms() {
        return alertSms;
    }

    public void setAlertSms(String alertSms) {
        this.alertSms = alertSms;
    }

    public String getAlertImm() {
        return alertImm;
    }

    public void setAlertImm(String alertImm) {
        this.alertImm = alertImm;
    }
}

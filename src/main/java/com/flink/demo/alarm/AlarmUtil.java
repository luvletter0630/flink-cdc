package com.flink.demo.alarm;

import com.alibaba.fastjson2.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

/**
 * @author liwj
 * @date 2024/4/10 15:39
 * @description:
 */
@Slf4j
public class AlarmUtil {
    private static String smsUrl = "http://172.30.141.134:8180/api/sendToSms";

    private static String immUrl = "http://172.30.141.134:8180/api/sendToImm";

    public static void sendForMessage(String phoneNumbers, String content , long jobId) {
        String postUrl = smsUrl;
        DefaultHttpClient client = new DefaultHttpClient();
        HttpPost post = new HttpPost(postUrl);
        JSONObject params = new JSONObject();
        params.put("phone", phoneNumbers);
        params.put("content", content);
        params.put("jobId", jobId);
        HttpResponse res;
        try {
            StringEntity s = new StringEntity(params.toString(), "utf-8");
            s.setContentType("application/json");//发送json数据需要设置contentType
            post.setEntity(s);
            res = client.execute(post);
            if (res.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                String result = EntityUtils.toString(res.getEntity());
                System.out.println(result);
            }else {
                String result = EntityUtils.toString(res.getEntity());
                throw new RuntimeException(result);
            }
        } catch (IOException arg13) {
            arg13.printStackTrace();
        }
    }

    public static void sendForImm(String userid, String content, long jobId) {
        String postUrl = immUrl;
        DefaultHttpClient client = new DefaultHttpClient();
        HttpPost post = new HttpPost(postUrl);
        JSONObject params = new JSONObject();
        params.put("userId", userid);
        params.put("content", content);
        params.put("jobId", jobId);
        HttpResponse res;
        try {
            StringEntity s = new StringEntity(params.toString(), "utf-8");
            s.setContentType("application/json");//发送json数据需要设置contentType
            post.setEntity(s);
            res = client.execute(post);
            if (res.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                String result = EntityUtils.toString(res.getEntity());
                System.out.println(result);
            }else {
                String result = EntityUtils.toString(res.getEntity());
                throw new RuntimeException(result);
            }
        } catch (IOException arg13) {
            arg13.printStackTrace();
        }
    }
}

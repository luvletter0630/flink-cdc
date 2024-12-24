package com.flink.demo.es;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author liwj
 * @date 2024/3/26 12:13
 * @description:
 */
public class EsSingleton {
    private static ElasticsearchClient esClient;

    private EsSingleton() {
    }

    public static ElasticsearchClient getEsClient() {
        if (esClient == null) {
            synchronized (EsSingleton.class) {
                if (esClient == null) {
                    esClient = getClient();
                }
            }
        }
        return esClient;
    }

    private static ElasticsearchClient getClient() {
        String esServerHosts = "172.30.141.122:9200,172.30.141.123:9200,172.30.141.124:9200";

        List<HttpHost> httpHosts = new ArrayList<>();
        //填充数据
        List<String> hostList = Arrays.asList(esServerHosts.split(","));
        for (int i = 0; i < hostList.size(); i++) {
            String host = hostList.get(i);
            httpHosts.add(new HttpHost(host.substring(0, host.indexOf(":")), Integer.parseInt(host.substring(host.indexOf(":") + 1)), "http"));
        }
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic", "primeton@000000"));
        RestClientBuilder.HttpClientConfigCallback callback = httpAsyncClientBuilder -> httpAsyncClientBuilder
                .setDefaultCredentialsProvider(credentialsProvider);
        // 创建低级客户端
        RestClient restClient = RestClient.builder(httpHosts.toArray(new HttpHost[3])).setHttpClientConfigCallback(callback).build();

        //使用Jackson映射器创建传输层
        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper()
        );

        ElasticsearchClient client = new ElasticsearchClient(transport);
        return client;
    }
}

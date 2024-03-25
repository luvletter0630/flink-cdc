package es;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
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
 * @author wei
 * @version 1.0
 * @date 2022/5/13 9:43
 */
public class EsClientPoolFactory implements PooledObjectFactory<ElasticsearchClient> {


    @Override
    public PooledObject<ElasticsearchClient> makeObject() throws Exception {

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
        return new DefaultPooledObject<>(client);
    }

    @Override
    public void destroyObject(PooledObject<ElasticsearchClient> p) throws Exception {
        ElasticsearchClient elasticsearchClient = p.getObject();
        //log.info("对象被销毁了" + elasticsearchClient);
    }

    @Override
    public boolean validateObject(PooledObject<ElasticsearchClient> p) {
        return true;
    }

    @Override
    public void activateObject(PooledObject<ElasticsearchClient> p) throws Exception {
        //log.info("对象被激活了" + p.getObject());
    }

    @Override
    public void passivateObject(PooledObject<ElasticsearchClient> p) throws Exception {
        //log.info("对象被钝化了" + p.getObject());
    }
}


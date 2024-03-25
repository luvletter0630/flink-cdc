package es;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.ElasticsearchIndicesClient;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author liwj
 * @date 2024/3/20 15:26
 * @description:
 */


@Slf4j
public class ESUtil {
    public static final Map<String, String> timeMap = new HashMap<>();

    public void insertESData(String index, JSONObject msg) throws Exception {

        boolean b = batchInsertDocument(index, Collections.singletonList(msg));
    }

    public boolean batchInsertDocument(String indexName, List<JSONObject> objs) throws Exception {
        ElasticsearchClient client = null;
        try {
            client = ESClientPool.getClient();
            IndexResponse indexResponse = client.index(
                    x -> x.index(indexName).document(objs.get(0)));
            return true;
        } catch (Exception e) {
            ESClientPool.returnClient(client);
            e.printStackTrace();
        }
        return false;
    }

    public String getDocumentsById(String index, String id) throws Exception {
        ElasticsearchClient client = ESClientPool.getClient();
        GetResponse getResponse = client.get(a -> a.index(index), Map.class);
        Object source = getResponse.source();
        if (source == null) {
            return null;
        }
        return JSON.toJSONString(source);
    }


    /**
     * 判断索引是否存在
     *
     * @param indexName
     * @return
     * @throws IOException
     */
    public boolean hasIndex(String indexName) {
        ElasticsearchClient client = null;
        BooleanResponse exists = null;
        try {
            client = ESClientPool.getClient();
            exists = client.indices().exists(d -> d.index(indexName));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return exists.value();
    }

    /**
     * 创建索引
     *
     * @param indexName
     * @return
     * @throws IOException
     */
    public boolean createIndex(String indexName, String indexJson) {
        ElasticsearchClient client;
        try {
            client = ESClientPool.getClient();
            ElasticsearchIndicesClient indices = client.indices();

            InputStream inputStream = this.getClass().getResourceAsStream(indexJson);


            CreateIndexRequest req = CreateIndexRequest.of(b -> b
                    .index(indexName)
                    .withJson(inputStream)
            );
            CreateIndexResponse indexResponse = indices.create(req);
        } catch (Exception e) {
            log.error("索引创建失败：{}", e.getMessage());
            throw new RuntimeException("创建索引失败");
        }
        return true;
    }


    public boolean insertData(JSONObject data, String indexName) throws Exception {
        String time = (DateTimeFormatter.BASIC_ISO_DATE).format(LocalDateTime.now());
        if (!StringUtils.isEmpty(time)) {
            String indexTime = time;
            indexName = indexName + "-" + indexTime;
            if (timeMap.get(indexName) == null) {
                timeMap.put(indexName, time);
                if (!hasIndex(indexName)) {
                    createIndex(indexName, "/flinkjobindex.json");
                }
            }

        }
        insertESData(indexName, data);
        return true;
    }

    public void searchEs() {
        ElasticsearchClient client;
        try {
            client = ESClientPool.getClient();
            SearchResponse searchResponse = client.search(srBuilder -> srBuilder
                            .index("flink-job-*").sort(sortOptionsBuilder -> sortOptionsBuilder.
                                    field(fieldSortBuilder -> fieldSortBuilder.field("time").
                                            order(SortOrder.Desc)))
                            .query(queryBuilder -> queryBuilder
                                    .match(matchQueryBuilder -> matchQueryBuilder
                                            .field("jobId").
                                            query("9e43c9af474558239b67ecfe2501abe2")))

                    , Map.class);

            HitsMetadata hits = searchResponse.hits();
            System.out.println("符合条件的总文档数量：" + hits.total().value());
            List<Hit> hitList = searchResponse.hits().hits(); //注意：第一个hits() 与 第二个hits()的区别

            for (Hit mapHit : hitList) {

                String source = mapHit.source().toString();

                System.out.println("文档原生信息：" + source);
            }
        } catch (Exception e) {
            log.error("索引创建失败：{}", e.getMessage());
            throw new RuntimeException("创建索引失败");
        }
    }

    public static void main(String[] args) {
        ESUtil esUtil = new ESUtil();
        try {
            esUtil.searchEs();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

package es;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wei
 * @version 1.0
 * @date 2022/5/16 15:47
 */
public class ESClientPool {

    private static Logger logger = LoggerFactory.getLogger(ESClientPool.class);

    // 对象池配置类，不写也可以，采用默认配置
    private static GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();

    // 采用默认配置maxTotal是8，池中有8个client
    static {
        poolConfig.setMaxIdle(20);
        poolConfig.setMaxTotal(8);
        poolConfig.setMinEvictableIdleTimeMillis(1000L*3L);
    }


    // 要池化的对象的工厂类，这个是我们要实现的类
    private static EsClientPoolFactory esClientPoolFactory = new EsClientPoolFactory();

    // 利用对象工厂类和配置类生成对象池
    private static GenericObjectPool<ElasticsearchClient> clientPool = new GenericObjectPool<>(esClientPoolFactory, poolConfig);


    /**
     * 获得对象
     *
     * @return
     * @throws Exception
     */
    public static ElasticsearchClient getClient() throws Exception {
        ElasticsearchClient client = clientPool.borrowObject();
        logger.info("从池中取一个对象"+client);
        return client;
    }

    /**
     * 归还对象
     *
     * @param client
     */
    public static void returnClient(ElasticsearchClient client) throws Exception {
        logger.info("使用完毕之后，归还对象"+client);
        clientPool.returnObject(client);
    }

}

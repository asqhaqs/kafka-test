
package soc.storm.situation.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.highlight.HighlightField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import soc.storm.situation.contants.SystemConstants;

/**
 * 
 * @author wangbin03
 *
 */
public class EsUtil {
    private static final Logger logger = LoggerFactory.getLogger(EsUtil.class);

    private static Client client = null;

    static {
        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", SystemConstants.ES_CLUSTER_NAME)
                .put("client.transport.sniff", true)
                // .put("client.transport.ping_timeout", "120s")
                .build();

        try {
            // 10.74.12.83 es-5.2-test "10.187.101.154" ES
            client = TransportClient
                    .builder()
                    .settings(settings)
                    .build()
                    .addTransportAddress(
                        new InetSocketTransportAddress(InetAddress.getByName(SystemConstants.ES_IP_ADDRESS), Integer
                                .parseInt(SystemConstants.ES_TCP_PORT)));

        } catch (UnknownHostException e) {
            throw new RuntimeException("init ES client error，" + e.getMessage());
        }
    }

    /**
     * 创建搜索引擎文档
     * 
     * @param index
     * @param type
     * @param doc
     * @param refresh
     * @return
     */
    public static String addIndex(String index, String type, Map<String, Object> doc, boolean refresh) {
        IndexResponse response = client.prepareIndex(index, type).setSource(doc).setRefresh(refresh).get();
        return response.getId();
    }

    /**
     * 创建搜索引擎文档
     * 
     * @param index 索引名称
     * @param type 索引类型
     * @param id 索引id
     * @param doc
     * @param refresh
     * @return
     */
    public static String saveDoc(String index, String type, String id, Map<String, Object> doc, boolean refresh) {
        IndexResponse response = client.prepareIndex(index, type, id).setSource(doc).setRefresh(refresh).get();
        return response.getId();
    }

    /**
     * 更新文档
     *
     * @param index
     * @param type
     * @param id
     * @param doc
     * @param refresh
     * @return
     */
    public static String updateDoc(String index, String type, String id, Map<String, Object> doc, boolean refresh) {
        UpdateResponse response = client.prepareUpdate(index, type, id).setDoc(doc).setRefresh(refresh).get();
        return response.getId();
    }

    /**
     * 删除索引
     *
     * @param index
     * @param type
     * @param id
     * @param refresh
     * @return
     */
    public static String deleteById(String index, String type, String id, boolean refresh) {
        DeleteResponse response = client.prepareDelete(index, type, id).setRefresh(refresh).get();
        return response.getId();
    }

    /**
     * 获取索引对应的存储内容
     *
     * @param index
     * @param type
     * @param id
     * @return
     */
    public static String getIdx(String index, String type, String id) {
        GetResponse response = client.prepareGet(index, type, id).get();
        if (response.isExists()) {
            return response.getSourceAsString();
        }
        return null;
    }

    /**
     * 判断某个索引下type是否存在
     *
     * @param index
     * @param type
     * @return
     */
    public static boolean isTypeExist(String index, String type) {
        return client.admin().indices().prepareTypesExists(index).setTypes(type).get().isExists();
    }

    /**
     * 判断索引是否存在
     *
     * @param index
     * @return
     */
    public static boolean isIndexExist(String index) {
        return client.admin().indices().prepareExists(index).get().isExists();
    }

    /**
     * 创建type（存在则进行更新）
     *
     * @param index 索引名称
     * @param type type名称
     * @param o 要设置type的object
     * @return
     */
    public static boolean createType(String index, String type, Object o) {
        if (!isIndexExist(index)) {
            logger.error("[createType] error, index={} is not exist", index);
            return false;
        }
        try {
            // 若type存在则可通过该方法更新type
            return client.admin().indices().preparePutMapping(index).setType(type).setSource(o).get().isAcknowledged();
        } catch (Exception e) {
            logger.error("[createType] error，{}", e.getMessage(), e);
            return false;
        }
    }

    /**
     * 查询示例
     * 
     * @param key
     * @param index
     * @param type
     * @param start
     * @param row
     * @return
     */
    public static Map<String, Object> search(String key, String index, String type, int start, int row) {
        SearchRequestBuilder builder = client.prepareSearch(index);
        builder.setTypes(type);
        builder.setFrom(start);
        builder.setSize(row);
        // 设置高亮字段名称
        builder.addHighlightedField("title");
        builder.addHighlightedField("describe");
        // 设置高亮前缀
        builder.setHighlighterPreTags("<font color='red' >");
        // 设置高亮后缀
        builder.setHighlighterPostTags("</font>");
        builder.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
        if (StringUtils.isNotBlank(key)) {
            // builder.setQuery(QueryBuilders.termQuery("title",key));
            builder.setQuery(QueryBuilders.multiMatchQuery(key, "title", "describe"));
        }
        builder.setExplain(true);
        SearchResponse searchResponse = builder.get();

        SearchHits hits = searchResponse.getHits();
        long total = hits.getTotalHits();
        Map<String, Object> map = new HashMap<String, Object>();
        SearchHit[] hits2 = hits.getHits();
        map.put("count", total);
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        for (SearchHit searchHit : hits2) {
            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
            HighlightField highlightField = highlightFields.get("title");
            Map<String, Object> source = searchHit.getSource();
            if (highlightField != null) {
                Text[] fragments = highlightField.fragments();
                String name = "";
                for (Text text : fragments) {
                    name += text;
                }
                source.put("title", name);
            }
            HighlightField highlightField2 = highlightFields.get("describe");
            if (highlightField2 != null) {
                Text[] fragments = highlightField2.fragments();
                String describe = "";
                for (Text text : fragments) {
                    describe += text;
                }
                source.put("describe", describe);
            }
            list.add(source);
        }

        map.put("dataList", list);
        return map;
    }

    /**
     * 获取eventId对应的situation-event存储内容
     * 
     * @param eventId
     * @return
     */
    public static Map<String, Object> getSituationEventByEventId(String eventId) {
        // logger.info("[getSituationEventByEventId] receive parameters: eventId={}", eventId);
        if (StringUtils.isBlank(eventId)) {
            return null;
        }

        SearchRequestBuilder builder = client.prepareSearch("situation-event");
        builder.setTypes("situation-event");
        builder.setFrom(0);
        builder.setSize(1);
        builder.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
        if (StringUtils.isNotBlank(eventId)) {
            builder.setQuery(QueryBuilders.termQuery("event_id", eventId));
        }
        builder.setExplain(false);
        SearchResponse searchResponse = builder.get();

        SearchHits hits = searchResponse.getHits();
        long total = hits.getTotalHits();
        if (0 == total) {
            return null;
        }

        Map<String, Object> situationEvent = hits.getHits()[0].getSource();
        situationEvent.put("id", hits.getHits()[0].getId());

        return situationEvent;
    }

    /**
     * 
     */
    public static void deleteByTerm() {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        SearchResponse response = client
                .prepareSearch("situation-event")
                .setTypes("situation-event")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termQuery("first_name", "xiaoming"))
                .setFrom(0)
                .setSize(20)
                .setExplain(true)
                .execute()
                .actionGet();

        for (SearchHit hit : response.getHits()) {
            String id = hit.getId();
            bulkRequest.add(client.prepareDelete("megacorp", "employee", id).request());
        }

        BulkResponse bulkResponse = bulkRequest.get();
        if (bulkResponse.hasFailures()) {
            for (BulkItemResponse item : bulkResponse.getItems()) {
                System.out.println(item.getFailureMessage());
            }
        } else {
            System.out.println("delete ok");
        }

    }

    /**
     * 
     * @param index
     * @param type
     * @param situationEventCache
     * @param refresh
     */
    public static void flushToEs(String index, String type, Map<Integer, Map<String, Object>> situationEventCache,
        boolean refresh) {
        // String index = "situation-event";
        // String type = "situation-event";

        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (Map.Entry<Integer, Map<String, Object>> situationEventEntry : situationEventCache.entrySet()) {
            Map<String, Object> situationEvent = situationEventEntry.getValue();
            bulkRequest.add(client.prepareIndex(index, type).setSource(situationEvent));
        }

        //
        BulkResponse bulkResponse = bulkRequest.setRefresh(refresh).get();
        // 打印错误日志
        if (bulkResponse.hasFailures()) {
            for (BulkItemResponse item : bulkResponse.getItems()) {
                logger.error("[flushToEs] error:{}", item.getFailureMessage());
            }
        }

    }
}

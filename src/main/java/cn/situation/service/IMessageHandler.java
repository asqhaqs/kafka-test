package cn.situation.service;

import java.util.List;
import java.util.Map;

public interface IMessageHandler {

    public Map<String, String> addMessageToBatch(String inputMessage, String indexName, String indexType) throws Exception;

    public String transformMessage(String inputMessage, Long offset) throws Exception;

    public boolean postToElasticSearch(List<Object> dataList) throws Exception;

    public boolean postToRedis(String key, List<String> dataList) throws Exception;

}

package cn.situation.service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public interface IMessageHandler {

    public JSONObject addMessageToBatch(String inputMessage, String indexName, String indexType) throws Exception;

    public String transformMessage(String inputMessage, Long offset) throws Exception;

    public boolean postToElasticSearch(JSONArray array) throws Exception;
    


}

package cn.situation.service.impl;

import cn.situation.service.IMessageHandler;
import cn.situation.util.HttpClientUtil;
import cn.situation.util.LogUtil;
import cn.situation.util.StringUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Value;

public class MessageHandlerImpl implements IMessageHandler {

    private static final Logger LOG = LogUtil.getInstance(MessageHandlerImpl.class);

	@Value("${es.bulk.url.list}")
	private String esBulkUrlList;

	@Override
	public String transformMessage(String inputMessage, Long offset) throws Exception {
		//TODO FILTER MAPPING
		return inputMessage;
	}

	@Override
	public JSONObject addMessageToBatch(String inputMessage, String indexName, String indexType) throws Exception {
		JSONObject jsonObject = new JSONObject();
		jsonObject.put("index", indexName);
		jsonObject.put("type", indexType);
		jsonObject.put("source", inputMessage);
		return jsonObject;
	}

	@Override
	public boolean postToElasticSearch(JSONArray array) throws Exception {
	    boolean result = false;
        JSONObject bulkData = new JSONObject();
        bulkData.put("data", array.toJSONString());
        String[] esBulkUrls = esBulkUrlList.split(",");
        for (String esBulkUrl : esBulkUrls) {
            String resp = HttpClientUtil.doPost(esBulkUrl, bulkData.toJSONString());
            LOG.info(String.format("[%s]: esBulkUrl<%s>, resp<%s>", "postToElasticSearch", esBulkUrl, resp));
            if (!StringUtil.isBlank(resp)) {
				JSONObject obj = JSONObject.parseObject(resp);
				if (obj.getBoolean("result")) {
					result = true;
				}
			}
        }
		return result;
	}
}

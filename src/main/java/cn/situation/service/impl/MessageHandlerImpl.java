package cn.situation.service.impl;

import cn.situation.service.IMessageHandler;
import cn.situation.util.HttpClientUtil;
import cn.situation.util.LogUtil;
import cn.situation.util.RedisCache;
import cn.situation.util.StringUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MessageHandlerImpl implements IMessageHandler {

    private static final Logger LOG = LogUtil.getInstance(MessageHandlerImpl.class);

	@Value("${es.bulk.url.list}")
	private String esBulkUrlList;

	@Value("${es.index.judge.exist}")
	private String isNeedJudgeIndex;

	@Autowired
	private RedisCache<String, Object> redisCache;

	@Override
	public String transformMessage(String inputMessage, Long offset) throws Exception {
		//TODO FILTER MAPPING
		return inputMessage;
	}

	@Override
	public Map<String, String> addMessageToBatch(String inputMessage, String indexName, String indexType) throws Exception {
		Map<String, String> map = new HashMap<>();
		map.put("index", indexName);
		map.put("type", indexType);
		map.put("source", inputMessage);
		map.put("isNeedJudgeIndex", isNeedJudgeIndex);
		return map;
	}

	@Override
	public boolean postToElasticSearch(List<Object> dataList) throws Exception {
	    boolean result = false;
	    Map<String, String> bulkMap = new HashMap<>();
	    bulkMap.put("data", JSONArray.toJSONString(dataList));
        String[] esBulkUrls = esBulkUrlList.split(",");
        for (String esBulkUrl : esBulkUrls) {
            String resp = HttpClientUtil.doPost(esBulkUrl, JSONObject.toJSONString(bulkMap));
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

	@Override
	public boolean postToRedis(String key, List<Object> dataList) throws Exception {
		return redisCache.rpushList(key, dataList);
	}
}

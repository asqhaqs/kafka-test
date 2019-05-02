package cn.situation.service.impl;

import cn.situation.exception.IndexerESNotRecoverableException;
import cn.situation.exception.IndexerESRecoverableException;
import cn.situation.service.ElasticSearchBatchService;
import cn.situation.service.IMessageHandler;
import org.springframework.beans.factory.annotation.Autowired;

public class MessageHandlerImpl implements IMessageHandler {

	@Autowired
	private ElasticSearchBatchService elasticSearchBatchService = null;

	@Override
	public String transformMessage(String inputMessage, Long offset) throws Exception {
		//TODO FILTER MAPPING
		return inputMessage;
	}

	@Override
	public void addMessageToBatch(String inputMessage, String indexName, String indexType) throws Exception {
		elasticSearchBatchService.addEventToBulkRequest(inputMessage, indexName, indexType);
	}

	@Override
	public void postToElasticSearch() throws InterruptedException, IndexerESRecoverableException,
			IndexerESNotRecoverableException {
		elasticSearchBatchService.postToElasticSearch();
	}
}

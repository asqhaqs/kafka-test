package cn.situation.service;

import cn.situation.exception.IndexerESNotRecoverableException;
import cn.situation.exception.IndexerESRecoverableException;

public interface IMessageHandler {

    public void addMessageToBatch(String inputMessage, String indexName, String indexType) throws Exception;

    public String transformMessage(String inputMessage, Long offset) throws Exception;

    public void postToElasticSearch() throws InterruptedException, IndexerESRecoverableException, IndexerESNotRecoverableException;
    


}

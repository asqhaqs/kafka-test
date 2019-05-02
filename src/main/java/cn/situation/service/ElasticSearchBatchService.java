package cn.situation.service;

import cn.situation.exception.IndexerESNotRecoverableException;
import cn.situation.exception.IndexerESRecoverableException;
import cn.situation.util.LogUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@Service
@Scope(value = BeanDefinition.SCOPE_PROTOTYPE)
public class ElasticSearchBatchService {
    private static final Logger LOG = LogUtil.getInstance(ElasticSearchBatchService.class);
    private static final String SERVICE_UNAVAILABLE = "SERVICE_UNAVAILABLE";
    private static final String INTERNAL_SERVER_ERROR = "INTERNAL_SERVER_ERROR";
    private BulkRequestBuilder bulkRequestBuilder;
    private Set<String> indexNames = new HashSet<>();
   
    @Value("${elasticsearch.reconnect.attempt.wait.ms:10000}")
    private long sleepBetweenESReconnectAttempts;
    
    @Autowired
    private ElasticSearchClientService elasticSearchClientService;

    private void initBulkRequestBuilder(){
    	if (bulkRequestBuilder == null){
    		bulkRequestBuilder = elasticSearchClientService.prepareBulk();
    	}
    }

    public void addEventToBulkRequest(String inputMessage, String indexName, String indexType) throws ExecutionException {
    	initBulkRequestBuilder();
        IndexRequestBuilder indexRequestBuilder = elasticSearchClientService.prepareIndex(indexName, indexType);
        indexRequestBuilder.setSource(inputMessage, XContentType.JSON);
        bulkRequestBuilder.add(indexRequestBuilder);
        indexNames.add(indexName);
    }

	public void postToElasticSearch() throws InterruptedException, IndexerESRecoverableException, IndexerESNotRecoverableException {
		try {
			if (bulkRequestBuilder != null) {
                LOG.info(String.format("[%s]: message<%s>", "postToElasticSearch", "Starting bulk posts to ES"));
				postBulkToEs(bulkRequestBuilder);
				LOG.info(String.format("[%s]: indexNames<%s>, message<%s>", "postToElasticSearch",
                        indexNames, bulkRequestBuilder.numberOfActions()));
			}
		} finally {
			bulkRequestBuilder = null;
			indexNames.clear();
		}
	}

    private void postBulkToEs(BulkRequestBuilder bulkRequestBuilder)
            throws InterruptedException, IndexerESRecoverableException, IndexerESNotRecoverableException {
        BulkResponse bulkResponse = null;
        BulkItemResponse bulkItemResp = null;
        if (bulkRequestBuilder.numberOfActions() <= 0) {
            LOG.warn(String.format("[%s]: message<%s>", "postBulkToEs", "No message to post ES"));
            return;
        }
        try {
            bulkResponse = bulkRequestBuilder.execute().actionGet();
        } catch (NoNodeAvailableException e) {
            LOG.error(e.getMessage(), e);
            elasticSearchClientService.reInitElasticSearch();
            throw new IndexerESRecoverableException("Recovering after an NoNodeAvailableException posting messages to ES " +
                    " - will re-try processing current batch");
        } catch (ElasticsearchException e) {
            LOG.error(e.getMessage(), e);
            throw new IndexerESRecoverableException(e);
        }
        LOG.info(String.format("[%s]: took<%s>, message<%s>", "postBulkToEs",
                bulkResponse.getIngestTookInMillis(), "Time to post messages to ES"));
        if (bulkResponse.hasFailures()) {
            LOG.error(String.format("[%s]: message<%s>", "postBulkToEs", bulkResponse.buildFailureMessage()));
            int failedCount = 0;
            Iterator<BulkItemResponse> bulkRespItr = bulkResponse.iterator();
            while (bulkRespItr.hasNext()) {
                bulkItemResp = bulkRespItr.next();
                if (bulkItemResp.isFailed()) {
                    failedCount++;
                    String errorMessage = bulkItemResp.getFailure().getMessage();
                    String restResponse = bulkItemResp.getFailure().getStatus().name();
                    LOG.error(String.format("[%s]: failedCount<%s>, restResponse<%s>, errorMessage<%s>", "postBulkToEs",
                            failedCount, restResponse, errorMessage));
                    if (SERVICE_UNAVAILABLE.equals(restResponse) || INTERNAL_SERVER_ERROR.equals(restResponse)){
                    	LOG.error("ES cluster unavailable, thread is sleeping for {} ms, after this current batch will be reprocessed",
                    			sleepBetweenESReconnectAttempts);
                    	Thread.sleep(sleepBetweenESReconnectAttempts);
                    	throw new IndexerESRecoverableException("Recovering after an SERVICE_UNAVAILABLE response from ES " +
                                " - will re-try processing current batch");
                    }
                }
            }
            LOG.error(String.format("[%s]: failedCount<%S>, message<%s>", "postBulkToEs", failedCount, "failed to post messages to ES"));
        }
    }

    public ElasticSearchClientService getElasticSearchClientService() {
        return elasticSearchClientService;
    }

    public void setElasticSearchClientService(ElasticSearchClientService elasticSearchClientService) {
        this.elasticSearchClientService = elasticSearchClientService;
    }
}
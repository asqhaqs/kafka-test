package cn.situation.service;

import cn.situation.exception.IndexerESNotRecoverableException;
import cn.situation.util.LogUtil;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.net.InetSocketAddress;
import java.util.List;

@Service
public class ElasticSearchClientService {

    private static final Logger LOG = LogUtil.getInstance(ElasticSearchClientService.class);
    private static final String CLUSTER_NAME = "cluster.name";

    @Value("${elasticsearch.cluster.name:elasticsearch}")
    private String esClusterName;
    @Value("#{'${elasticsearch.hosts.list:localhost:9300}'.split(',')}")
    private List<String> esHostPortList;
    @Value("${elasticsearch.indexing.retry.sleep.ms:10000}")
    private   int esIndexingRetrySleepTimeMs;
    @Value("${elasticsearch.indexing.retry.attempts:2}")
    private   int numberOfEsIndexingRetryAttempts;

	private TransportClient esTransportClient;

    @PostConstruct
    public void init() throws Exception {
    	LOG.info("Initializing ElasticSearchClient ...");
		Settings setting = Settings.builder()
				.put(CLUSTER_NAME, esClusterName)
				.put("client.transport.sniff", false)
				.put("client.transport.ping_timeout", "60s")
				.put("client.transport.nodes_sampler_interval", "60s")
				.build();
        try {
            esTransportClient  = new PreBuiltTransportClient(setting);
            for (String eachHostPort : esHostPortList) {
            	LOG.info(String.format("[%s]: eachHostPort<%s>", "init", eachHostPort));
                String[] hostPortTokens = eachHostPort.split(":");
                if (hostPortTokens.length < 2) 
                	throw new Exception("ERROR: bad ES host:port configuration: " + eachHostPort);
                int port = 9300;
                try {
                	port = Integer.parseInt(hostPortTokens[1].trim());
                } catch (Throwable e){
                	LOG.error("ERROR parsing port from the ES config [{}] - using default port 9300", eachHostPort);
                }
                esTransportClient.addTransportAddress(new InetSocketTransportAddress(
                		new InetSocketAddress(hostPortTokens[0].trim(), port)));
            }
            LOG.info("ES Client created and intialized OK");
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

	@PreDestroy
    public void cleanup() throws Exception {
		if (esTransportClient != null)
			esTransportClient.close();
    }
    
	public void reInitElasticSearch() throws InterruptedException, IndexerESNotRecoverableException {
		for (int i=1; i<=numberOfEsIndexingRetryAttempts; i++ ){
			Thread.sleep(esIndexingRetrySleepTimeMs);
			LOG.warn(String.format("[%s]: num<%s>", "reInitElasticSearch", i));
			try {
				init();
				return;
			} catch (Exception e) {
				if (i < numberOfEsIndexingRetryAttempts) {
					LOG.warn(String.format("[%s]: num<%s>, message<%s>", "reInitElasticSearch", i, e.getMessage()));
				} else {
					LOG.error(String.format("[%s]: num<%s>", "reInitElasticSearch", i));
					throw new IndexerESNotRecoverableException("ERROR: failed to connect to ES after max number of retries ");
				}
			}
		}
	}

	public void deleteIndex(String index) {
		esTransportClient.admin().indices().prepareDelete(index).execute().actionGet();
		LOG.info(String.format("[%s]: index<%s>, message<%s>", "deleteIndex", index, "Delete index success"));
	}

	public void createIndex(String indexName){
		esTransportClient.admin().indices().prepareCreate(indexName).execute().actionGet();
		LOG.info(String.format("[%s]: index<%s>, message<%s>", "createIndex", indexName, "Created index success"));
	}

	public void createIndexAndAlias(String indexName,String aliasName){
		esTransportClient.admin().indices().prepareCreate(indexName).addAlias(new Alias(aliasName)).execute().actionGet();
		LOG.info(String.format("[%s]: index<%s>, alias<%s>", "createIndexAndAlias", indexName, aliasName));
	}

	public void addAliasToExistingIndex(String indexName, String aliasName) {
		esTransportClient.admin().indices().prepareAliases().addAlias(indexName, aliasName).execute().actionGet();
		LOG.info(String.format("[%s]: alias<%s>, index<%s>", "addAliasToExistingIndex", aliasName, indexName));
	}

	public IndexRequestBuilder prepareIndex(String indexName, String indexType) {
		return esTransportClient.prepareIndex(indexName, indexType);
	}

	public BulkRequestBuilder prepareBulk() {
		return esTransportClient.prepareBulk();
	}

	public TransportClient getEsTransportClient() {
		return esTransportClient;
	}
}
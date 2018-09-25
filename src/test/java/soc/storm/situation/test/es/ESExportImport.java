
package soc.storm.situation.test.es;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;

/**
 * 
 * @author wangbin03
 *
 */
public class ESExportImport {

    /**
     * ElasticSearch carry
     * 
     * @param clustName
     * @param indexName
     * @param indexType
     * @param destiClustName
     * @param destiIndexName
     * @param destiIndexType
     * @throws InterruptedException
     */
    public static void executeRecreate(String clustName, String indexName, String indexType,
        String destiClustName, String destiIndexName, String destiIndexType) {

        // build source settings
        Settings settings = Settings.builder()
                .put("cluster.name", clustName).put("client.transport.sniff", true)
                .put("client.transport.ping_timeout", "30s")
                .put("client.transport.nodes_sampler_interval", "30s").build();
        TransportClient client = TransportClient.builder().settings(settings).build();
        client.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("10.95.38.21", 9300)));

        // build destination settings
        Settings destiSettings = Settings.builder()
                .put("cluster.name", destiClustName)
                .put("client.transport.sniff", true)
                .put("client.transport.ping_timeout", "30s")
                .put("client.transport.nodes_sampler_interval", "30s").build();
        TransportClient destiClient = TransportClient.builder().settings(destiSettings).build();
        destiClient.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("172.24.2.157", 9300)));

        SearchResponse scrollResp = client.prepareSearch(indexName).setScroll(new TimeValue(30000)).setSize(1000).execute().actionGet();

        // build destination bulk
        BulkRequestBuilder bulk = destiClient.prepareBulk();

        ExecutorService executor = Executors.newFixedThreadPool(5);
        while (true) {
            bulk = destiClient.prepareBulk();
            final BulkRequestBuilder bulkRequest = bulk;
            boolean isHaveLog = false;
            for (SearchHit hit : scrollResp.getHits().getHits()) {
                isHaveLog = true;
                IndexRequest req = destiClient.prepareIndex().setIndex(destiIndexName)
                        .setType(destiIndexType).setSource(hit.getSourceAsString()).request();
                bulkRequest.add(req);
            }

            if (isHaveLog) {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        // bulkRequest.execute().get();
                        //
                        BulkResponse bulkResponse = bulkRequest.get();
                        // 打印错误日志
                        if (bulkResponse.hasFailures()) {
                            for (BulkItemResponse item : bulkResponse.getItems()) {
                                System.out.println("[flushToEs] error:" + item.getFailureMessage());
                            }
                        }
                    }
                });
            }

            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId())
                    .setScroll(new TimeValue(30000)).execute().actionGet();
            if (scrollResp.getHits().getHits().length == 0) {
                break;
            }
        }
    }

    public static void main(String[] args) {

        // situation-ids.tmpl
        // situation-latent_danger_log.tmpl
        // situation-latent_danger.tmpl
        // situation-malware_log.tmpl
        // situation-malware.tmpl
        // situation-sys_log.tmpl
        // situation-tamper_log.tmpl
        // situation-tamper.tmpl
        // situation-usability_log.tmpl
        // situation-usability.tmpl
        // situation-vulnerability_log.tmpl
        // situation-vulnerability.tmpl
        // situation-web_fishing_log.tmpl
        // situation-web_fishing.tmpl
        // situation-web_trojan.tmpl

        // formatting.tmpl
        // executeRecreate("es", "formatting", "formatting", "esclustername", "formatting", "formatting");

        // situation-advanced_threat.tmpl
        // executeRecreate("es", "situation-advanced_threat", "situation-advanced_threat", "esclustername",
        // "situation-advanced_threat",
        // "situation-advanced_threat");

        // situation-ddos_log.tmpl
        // executeRecreate("es", "situation-ddos_log", "situation-ddos_log", "esclustername", "situation-ddos_log",
        // "situation-ddos_log");

        // situation-ddos.tmpl
        // executeRecreate("es", "situation-ddos", "situation-ddos", "esclustername", "situation-ddos",
        // "situation-ddos");

        // situation-event.tmpl
        // executeRecreate("es", "situation-event", "situation-event", "esclustername", "situation-event",
        // "situation-event");
        //
        // executeRecreate("es", "situation-ids", "situation-ids", "esclustername", "situation-ids", "situation-ids");
        // executeRecreate("es", "situation-latent_danger_log", "situation-latent_danger_log", "esclustername",
        // "situation-latent_danger_log",
        // "situation-latent_danger_log");
        // executeRecreate("es", "situation-latent_danger", "situation-latent_danger", "esclustername",
        // "situation-latent_danger",
        // "situation-latent_danger");
        // executeRecreate("es", "situation-malware_log", "situation-malware_log", "esclustername",
        // "situation-malware_log",
        // "situation-malware_log");
        // executeRecreate("es", "situation-malware", "situation-malware", "esclustername", "situation-malware",
        // "situation-malware");
        // executeRecreate("es", "situation-sys_log", "situation-sys_log", "esclustername", "situation-sys_log",
        // "situation-sys_log");
        // executeRecreate("es", "situation-tamper_log", "situation-tamper_log", "esclustername",
        // "situation-tamper_log",
        // "situation-tamper_log");
        // executeRecreate("es", "situation-tamper", "situation-tamper", "esclustername", "situation-tamper",
        // "situation-tamper");
        // executeRecreate("es", "situation-usability_log", "situation-usability_log", "esclustername",
        // "situation-usability_log",
        // "situation-usability_log");
        //
        // executeRecreate("es", "situation-usability", "situation-usability", "esclustername", "situation-usability",
        // "situation-usability");
        // executeRecreate("es", "situation-vulnerability_log", "situation-vulnerability_log", "esclustername",
        // "situation-vulnerability_log",
        // "situation-vulnerability_log");
        // executeRecreate("es", "situation-vulnerability", "situation-vulnerability", "esclustername",
        // "situation-vulnerability",
        // "situation-vulnerability");
        // executeRecreate("es", "situation-web_fishing_log", "situation-web_fishing_log", "esclustername",
        // "situation-web_fishing_log",
        // "situation-web_fishing_log");
        // executeRecreate("es", "situation-web_fishing", "situation-web_fishing", "esclustername",
        // "situation-web_fishing",
        // "situation-web_fishing");
    }

}

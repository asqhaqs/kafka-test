
package soc.storm.situation.monitor;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import soc.storm.situation.contants.SystemConstants;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import java.util.HashMap;
import java.util.Map;

public class ExtendIpEnrichmentTopology {

    private static final Logger logger = LoggerFactory.getLogger(ExtendIpEnrichmentTopology.class);

    // 各个组件名字的唯一标识
    private final static String KAFKA_CONSUMER_SPOUT_ID = "kafka_consumer_spout";
    private final static String IP_ENRICHMENT_BOLT_ID = "ip_enrichment_bolt";
    private final static String KAFKA_PRODUCER_BOLT_ID = "kafka_producer_bolt";

    // tolopy_name
    private final static String TOPOLOGY_NAME = SystemConstants.TOPOLOGY_NAME;// "extend_ip_enrichment_topology";

    public static void main(String[] args) {
        try {
            // 构建一个拓扑Builder
            TopologyBuilder topologyBuilder = new TopologyBuilder();

            // 线程数
            int KAFKA_SPOUT_THREADS = Integer.parseInt(SystemConstants.KAFKA_SPOUT_THREADS);
            int IP_ENRICHMENT_BOLT_THREADS = Integer.parseInt(SystemConstants.IP_ENRICHMENT_BOLT_THREADS);
            int KAFKA_BOLT_THREADS = Integer.parseInt(SystemConstants.KAFKA_BOLT_THREADS);

            //
            String[] topicNameInputArray = SystemConstants.TOPIC_NAME_INPUT.split(",");
            String[] topicNameOutputArray = SystemConstants.TOPIC_NAME_OUTPUT.split(",");

            for (int i = 0; i < topicNameInputArray.length; i++) {
                String topicNameInput = topicNameInputArray[i].trim();
                String topicNameOutput = topicNameOutputArray[i].trim();

                String zkRoot = "";
                // 配置zookeeper 主机:端口号
                BrokerHosts brokerHosts = new ZkHosts(SystemConstants.ZOOKEEPER_HOSTS);
                SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topicNameInput, zkRoot,
                        KAFKA_CONSUMER_SPOUT_ID + topicNameInput);
                // 设置如何处理kafka消息队列输入流
                spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());
                // spoutConfig.ignoreZkOffsets = true;// 从头开始消费
                // spoutConfig.forceFromStart = false; // 从头开始消费
                spoutConfig.socketTimeoutMs = 60 * 1000;
                // TODO:online delete
                // spoutConfig.startOffsetTime = OffsetRequest.LatestTime(); // -1

                // （1）KafkaConsumerSpout
                topologyBuilder.setSpout(KAFKA_CONSUMER_SPOUT_ID + topicNameInput, new KafkaSpout(spoutConfig),
                    KAFKA_SPOUT_THREADS);
                // KafkaConsumerSpout kafkaConsumerSpout = new KafkaConsumerSpout(topicNameInput);
                // topologyBuilder.setSpout(KAFKA_CONSUMER_SPOUT_ID + topicNameInput, kafkaConsumerSpout,
                // KAFKA_SPOUT_THREADS);

                // （2）IpEnrichmentBolt
                IpEnrichmentBolt ipEnrichmentBolt = new IpEnrichmentBolt(topicNameInput);
                topologyBuilder.setBolt(IP_ENRICHMENT_BOLT_ID + topicNameInput, ipEnrichmentBolt, IP_ENRICHMENT_BOLT_THREADS)
                        .shuffleGrouping(KAFKA_CONSUMER_SPOUT_ID + topicNameInput);

                // （3）KafkaProcuderBolt
                KafkaProcuderBolt kafkaProducerBolt = new KafkaProcuderBolt(topicNameOutput);
                topologyBuilder.setBolt(KAFKA_PRODUCER_BOLT_ID + topicNameInput, kafkaProducerBolt, KAFKA_BOLT_THREADS)
                        .shuffleGrouping(IP_ENRICHMENT_BOLT_ID + topicNameInput);
            }

            Config conf = new Config();
            // 是否输出调试信息
            conf.setDebug(Boolean.parseBoolean(SystemConstants.TOPOLOGY_DEBUG));

            Map<String, String> propertymap = new HashMap<String, String>();
            propertymap.put("metadata.broker.list", SystemConstants.BROKER_URL);
            propertymap.put("serializer.class", "kafka.serializer.StringEncoder");
            conf.put("kafka.broker.properties", propertymap);

            //
            if (args != null && args.length > 0) {
                // 远程集群
                conf.setNumWorkers(Integer.parseInt(SystemConstants.TOPOLOGY_WORKER_NUM));
                conf.setMaxSpoutPending(Integer.parseInt(SystemConstants.MAX_SPOUT_PENDING));
                StormSubmitter.submitTopologyWithProgressBar(TOPOLOGY_NAME, conf, topologyBuilder.createTopology());
            } else {
                // 建立本地集群,利用LocalCluster,storm在程序启动时会在本地自动建立一个集群,不需要用户自己再搭建,方便本地开发和debug
                LocalCluster cluster = new LocalCluster();
                // 创建拓扑实例,并提交到本地集群进行运行
                cluster.submitTopology(TOPOLOGY_NAME, conf, topologyBuilder.createTopology());
            }
        } catch (NumberFormatException e) {
            logger.error("NumberFormatException : {}", e.getMessage(), e);
        } catch (IllegalArgumentException e) {
            logger.error("IllegalArgumentException : {}", e.getMessage(), e);
        } catch (AlreadyAliveException e) {
            logger.error("AlreadyAliveException : {}", e.getMessage(), e);
        } catch (InvalidTopologyException e) {
            logger.error("InvalidTopologyException : {}", e.getMessage(), e);
        } catch (AuthorizationException e) {
            logger.error("AuthorizationException : {}", e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Exception : {}", e.getMessage(), e);
        }

    }
}

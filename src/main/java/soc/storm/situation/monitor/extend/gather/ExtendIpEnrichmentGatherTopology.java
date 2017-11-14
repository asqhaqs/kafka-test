
package soc.storm.situation.monitor.extend.gather;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import soc.storm.situation.contants.SystemConstants;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

public class ExtendIpEnrichmentGatherTopology {

    private static final Logger logger = LoggerFactory.getLogger(ExtendIpEnrichmentGatherTopology.class);

    // 各个组件名字的唯一标识
    private final static String KAFKA_CONSUMER_SPOUT_ID = "kafka_consumer_spout01";

    // tolopy_name
    private final static String TOPOLOGY_NAME = SystemConstants.TOPOLOGY_NAME;// "extend_ip_enrichment_topology";

    public static void main(String[] args) {
        try {
            // 构建一个拓扑Builder
            TopologyBuilder topologyBuilder = new TopologyBuilder();

            // 线程数
            int KAFKA_SPOUT_THREADS = Integer.parseInt(SystemConstants.KAFKA_SPOUT_THREADS);

            //
            String[] topicNameInputArray = SystemConstants.TOPIC_NAME_INPUT.split(",");
            String[] topicNameOutputArray = SystemConstants.TOPIC_NAME_OUTPUT.split(",");

            for (int i = 0; i < topicNameInputArray.length; i++) {
                String topicNameInput = topicNameInputArray[i].trim();
                String topicNameOutput = topicNameOutputArray[i].trim();

                //
                KafkaConsumerGatherSpout kafkaConsumerSpout = new KafkaConsumerGatherSpout(topicNameInput, topicNameInput, topicNameOutput);
                topologyBuilder.setSpout(KAFKA_CONSUMER_SPOUT_ID + topicNameInput, kafkaConsumerSpout,
                    KAFKA_SPOUT_THREADS);
            }

            Config conf = new Config();
            // 是否输出调试信息
            conf.setDebug(Boolean.parseBoolean(SystemConstants.TOPOLOGY_DEBUG));

            Map<String, String> propertymap = new HashMap<String, String>();
            propertymap.put("metadata.broker.list", SystemConstants.BROKER_URL);
            propertymap.put("serializer.class", "kafka.serializer.StringEncoder");
            conf.put("kafka.broker.properties", propertymap);

            // redis
            Jedis jedis = new Jedis("10.95.37.16", 6380);
            jedis.set("MessageCount", "0");
            jedis.close();

            //
            if (args != null && args.length > 0) {
                // 远程集群
                conf.setNumWorkers(Integer.parseInt(SystemConstants.TOPOLOGY_WORKER_NUM));
                conf.setMaxSpoutPending(Integer.parseInt(SystemConstants.MAX_SPOUT_PENDING));
                conf.setMessageTimeoutSecs(60);// acker failed 超时时间
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

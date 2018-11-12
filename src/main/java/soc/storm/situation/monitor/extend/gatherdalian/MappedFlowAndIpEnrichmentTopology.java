package soc.storm.situation.monitor.extend.gatherdalian;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import soc.storm.situation.contants.SystemMapEnrichConstants;

/**
 * storm拓扑  流量读取发射、流量解析筛选、流量映射富化、流量录入
 * @author xudong
 *
 */
public class MappedFlowAndIpEnrichmentTopology {
	
	private static final Logger logger = LoggerFactory.getLogger(MappedFlowAndIpEnrichmentTopology.class);
	
	//各个组件的唯一标识
	private final static String KAFKA_CONSUMER_SPOUT_ID = "kafka_consumer_spout";
	private final static String ANALYSIS_BOLT_ID = "analysis_bolt";
	private final static String MAPPING_ENRICHMENT_BOLT_ID = "mapping_enrichment_bolt";
	private final static String KAFKA_PRODUCER_BOLT_ID = "kafka_producer_bolt";
	
	//tolopy name
	private final static String TOPOLOGY_NAME = SystemMapEnrichConstants.TOPOLOGY_NAME;

	public static void main(String[] args) {
		
		try {
			// TODO 流量读取发射、流量解析删选、流量映射、流量录入
			TopologyBuilder topologyBuilder = new TopologyBuilder();
			
			//线程数
			int KAFKA_SPOUT_THREADS = Integer.parseInt(SystemMapEnrichConstants.KAFKA_SPOUT_THREADS);
			int ANALYSIS_BOLT_THREADS = Integer.parseInt(SystemMapEnrichConstants.ANALYSIS_BOLT_THREADS);
			int MAPPING_ENRICHMENT_BOLT_THREADS = Integer.parseInt(SystemMapEnrichConstants.MAPPING_ENRICHMENT_BOLT_THREADS);
			int KAFKA_BOLT_THREADS = Integer.parseInt(SystemMapEnrichConstants.KAFKA_BOLT_THREADS);
			
			//tpoic 数组
			String[] topicMapEnrichInputArray = SystemMapEnrichConstants.TOPIC_MAP_ENRICH_INPUT.split(",");
			String[] topicMapEnrichOutputArray = SystemMapEnrichConstants.TOPIC_MAP_ENRICH_OUTPUT.split(",");
			
			
			logger.info("---------------SystemMapEnrichConstants.TOPIC_MAP_ENRICH_INPUT: " + SystemMapEnrichConstants.TOPIC_MAP_ENRICH_INPUT);
			logger.info("---------------SystemMapEnrichConstants.TOPIC_MAP_ENRICH_OUTPUT: " + SystemMapEnrichConstants.TOPIC_MAP_ENRICH_OUTPUT);
			logger.info("---------------TopicMapEnrichInputArray length: " + topicMapEnrichInputArray.length);
			logger.info("---------------TopicMapEnrichOutputArray length: " + topicMapEnrichOutputArray.length);
			
			
			Map<String, MappingAndEnrichmentBolt> mapEnrichBolts = new HashMap<String, MappingAndEnrichmentBolt>();
			Map<String, KafkaProducerBolt> kafkaProducerBolts = new HashMap<String, KafkaProducerBolt>();
			// 连接 映射富化 和 发送的 bolt， 这两个按 topic_output进行分组 先初始化 对象
			for(int j = 0; j < topicMapEnrichOutputArray.length; j++){
				for(int i = 0; i < topicMapEnrichInputArray.length; i++) {
					String topicOutput = topicMapEnrichOutputArray[j].trim();
					String topicInput = topicMapEnrichInputArray[i].trim();
					String bolt_name = topicInput + ":" + topicOutput;
					MappingAndEnrichmentBolt mappingAndEnrichmentBolt = new MappingAndEnrichmentBolt(bolt_name);
					mapEnrichBolts.put(bolt_name, mappingAndEnrichmentBolt);
					
					logger.info("********kafkaProducerBolt init -----------");
					KafkaProducerBolt kafkaProducerBolt = new KafkaProducerBolt(bolt_name);
					logger.info("********kafkaProducerBolt init -----------");
					kafkaProducerBolts.put(bolt_name, kafkaProducerBolt);
				}
				
			}
			
			//检验 map初始化
			logger.info("------------------mapEnrichBolts size: " + mapEnrichBolts.size());
			
			// 消费 的 spout 和 解析的 bolt, 这两个按topic_input进行分组, 并组装拓扑
			for(int i = 0; i < topicMapEnrichInputArray.length; i++) {
				
				String topicInput = topicMapEnrichInputArray[i].trim();
				
				// kafka consumer spout
				KafkaConsumerSpout kafkaConsumerSpout = new KafkaConsumerSpout(topicInput);
				topologyBuilder.setSpout(KAFKA_CONSUMER_SPOUT_ID + topicInput, kafkaConsumerSpout, KAFKA_SPOUT_THREADS);
				
				AnalysisBolt analysisBolt = new AnalysisBolt(topicInput, ANALYSIS_BOLT_ID, MAPPING_ENRICHMENT_BOLT_ID);
				topologyBuilder.setBolt(ANALYSIS_BOLT_ID + topicInput, analysisBolt, ANALYSIS_BOLT_THREADS)
					.localOrShuffleGrouping(KAFKA_CONSUMER_SPOUT_ID + topicInput);
				
				//for(Map.Entry<String, MappingAndEnrichmentBolt> entry : mapEnrichBolts.entrySet()) {
				for(int j = 0; j < topicMapEnrichOutputArray.length; j++) {
					String topicOutput = topicMapEnrichOutputArray[j];
					String bolt_name = topicInput+ ":" +topicOutput;
					// 定义 analysis bolt   以及  mapping_enrichment bolt 的  连接拓扑， 以及 流 id
					logger.info("-----------------------the out topic name is-----{}---------------------", bolt_name.split(":")[1]);
					String analysisId = ANALYSIS_BOLT_ID + topicInput;
					String maprichId = MAPPING_ENRICHMENT_BOLT_ID + bolt_name;
					String streamId =  analysisId + maprichId;
					topologyBuilder.setBolt(maprichId, mapEnrichBolts.get(bolt_name), 
							MAPPING_ENRICHMENT_BOLT_THREADS).localOrShuffleGrouping(analysisId, 
									streamId);
					logger.info("***************the analysis is"+ analysisId + ";the mapenrich is: " + maprichId +
							";the streaming id is： " + streamId + "***********************");

					
					logger.info("--------------contect the producer boltid is " + KAFKA_PRODUCER_BOLT_ID + bolt_name);
					topologyBuilder.setBolt(KAFKA_PRODUCER_BOLT_ID + bolt_name, kafkaProducerBolts.get(bolt_name), 
							KAFKA_BOLT_THREADS).localOrShuffleGrouping(MAPPING_ENRICHMENT_BOLT_ID + bolt_name);
					
				}
			}
			
			Config config = new Config();
			//是否输出调试信息
			config.setDebug(Boolean.parseBoolean(SystemMapEnrichConstants.TOPOLOGY_DEBUG));
			
			Map<String, String> propertyMap = new HashMap<String, String>();
			propertyMap.put("metadata.broker.list", SystemMapEnrichConstants.BROKER_URL);
			propertyMap.put("serializer.class", "kafka.serializer.StringEncoder");
			config.put("kafka.broker.properties", propertyMap);
			
			if(args != null && args.length >0) {
				config.setNumWorkers(Integer.parseInt(SystemMapEnrichConstants.TOPOLOGY_WORKER_NUM));
	            // 设置一个spout task上面最多有多少个没有处理(ack/fail)的tuple，防止tuple队列过大, 只对可靠任务起作用
	            config.setMaxSpoutPending(Integer.parseInt(SystemMapEnrichConstants.MAX_SPOUT_PENDING));
	            //设置超时时间
	            config.setMessageTimeoutSecs(60);
	            config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 
	            		Integer.parseInt(SystemMapEnrichConstants.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE));
	            config.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 
	            		Integer.parseInt(SystemMapEnrichConstants.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE));
	            config.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, Integer.parseInt(SystemMapEnrichConstants.TOPOLOGY_TRANSFER_BUFFER_SIZE));
	            // 打印配置
	            logger.info("********************************TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE： " + config.get(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE));
	            StormSubmitter.submitTopologyWithProgressBar(TOPOLOGY_NAME, config, topologyBuilder.createTopology());
	            
			}else {
				// 本地测试
				LocalCluster local = new LocalCluster();
				local.submitTopology(TOPOLOGY_NAME, config, topologyBuilder.createTopology());
			}
		}catch(NumberFormatException e) {
			logger.error("NumberFormatException: {}", e.getMessage(), e);
		}catch(IllegalArgumentException e) {
			logger.error("IllegalArgumentException: {}", e.getMessage(), e);
		}catch(AlreadyAliveException e) {
			logger.error("AlreadyAliveException: {}", e.getMessage(), e);
		}catch(InvalidTopologyException e) {
			logger.error("InvalidTopologyException: {}", e.getMessage(), e);
		}catch(AuthorizationException e) {
			logger.error("AuthorizationException: {}", e.getMessage(), e);
		}catch(Exception e) {
			logger.error("Exception: {}", e.getMessage(), e);
		}
	
	}

}

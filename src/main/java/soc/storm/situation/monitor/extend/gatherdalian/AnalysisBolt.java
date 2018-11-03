package soc.storm.situation.monitor.extend.gatherdalian;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import soc.storm.situation.contants.SystemMapEnrichConstants;

/**
 *      删选出需要的几种流量类型 && 分发至各个富化bolt
 * @author xudong
 *
 */

public class AnalysisBolt extends BaseRichBolt {
	
	//手动指定序列化id，防止后续类修改导致反序列化失败
	private static final long serialVersionUID = -2639126860311224666L;
	
	private static final Logger logger = LoggerFactory.getLogger(AnalysisBolt.class);
	
	private OutputCollector outputCollector;
	
	private static String[] flowTypes;
	private static String[] topicOutputs;
	private static String[] typeMappingRules;
	private static String separator;
	
	// 应该是从日志中提取出多条日志的 数组的 方法
	private final String topicInput;
	
	//映射富化 bolt 组件的标识
	private final String MAPPING_ENRICHMENT_BOLT_ID;
	
	//分析 bolt 组件的 标识
	private final String ANALYSIS_BOLT_ID;
	
    static {
        System.out.println("--------------------AnalysisBolt-------------SystemMapEnrichConstants.BROKER_URL:" + SystemMapEnrichConstants.BROKER_URL);
        if (SystemMapEnrichConstants.IS_KERBEROS.equals("true")) {
            System.setProperty("java.security.auth.login.config",
            		SystemMapEnrichConstants.KAFKA_KERBEROS_PATH + File.separator + "kafka_server_jaas.conf");
            System.setProperty("java.security.krb5.conf", SystemMapEnrichConstants.KAFKA_KERBEROS_PATH + File.separator + "krb5.conf");
        }
    }
	
	static {
		//需要的 流量种类 & 输出topic & 金睛与360 流量 映射规则  &  金睛流量日志分隔符
		flowTypes = SystemMapEnrichConstants.FLOW_TYPES.split(",");
		topicOutputs = SystemMapEnrichConstants.TOPIC_MAP_ENRICH_OUTPUT.split(",");
		typeMappingRules = SystemMapEnrichConstants.TYPE_MAPPING_RULES.split(",");
		separator = SystemMapEnrichConstants.FLOW_LOG_SEPARATOR;
	}
	
	
	public AnalysisBolt(String topicInput, String analysisId, String maprichId) {
		
		this.topicInput = topicInput;
		this.MAPPING_ENRICHMENT_BOLT_ID = maprichId;
		this.ANALYSIS_BOLT_ID = analysisId;
		
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		
		this.outputCollector = collector;

	}

	@Override
	public void execute(Tuple tuple) {

		//测试执行时间
		long taskbegin = System.currentTimeMillis();
		//测试解析时间
		long analysisbegin = System.currentTimeMillis();
		//提取日志字段部分
		String syslogValue = (String)tuple.getValue(0);
		String[] syslogpre = syslogValue.split(separator.trim());
		String syslog = null;
		if(syslogpre.length == 2) {
			syslog = syslogpre[1];
		}
		List<String> fieldList = null;
		String type = null;
		if(StringUtils.isNotBlank(syslog)) {
			//截断 syslog 并进行 判断
			fieldList = Arrays.asList(syslog.split("\\^"));
			type = fieldList.get(3).trim();
			long analysisend = System.currentTimeMillis();
			logger.info("----------------------------- split time is: {} ms", (analysisend-analysisbegin));
		
			//判断该type是否是我们需要的类型 && 对相应的输出topic bolt进行分发   
			//这里使用了 下标映射 使其可配置 1.找到jj 流量类型在数组位置；2.找到其在对应关系中对应360类型的下标；3.转发至相应360类型的的 enrichment bolt处理 
			for(int i = 0; i < flowTypes.length; i++) {
				if(type.equals(flowTypes[i].trim())) {
					for(int j = 0; j < typeMappingRules.length; j++) {
						if(Integer.parseInt(typeMappingRules[j].trim()) == i) {
							String streamID = ANALYSIS_BOLT_ID + topicInput + MAPPING_ENRICHMENT_BOLT_ID + topicOutputs[j].trim();
							outputCollector.emit(streamID,tuple,new Values(type, fieldList));
						}
							
					}
				}
			}
			long taskend = System.currentTimeMillis();
			logger.info("----------------------------- analysis task time is: {} ms", (taskend-taskbegin));
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
		for (String topicOutput : topicOutputs) {
			String streamId = ANALYSIS_BOLT_ID + topicInput + MAPPING_ENRICHMENT_BOLT_ID + topicOutput.trim();
			declarer.declareStream(streamId, new Fields("type", "fieldList"));
		}

	}

}

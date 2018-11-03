package soc.storm.situation.monitor.extend.gatherdalian;

import java.io.File;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import soc.storm.situation.contants.SystemMapEnrichConstants;
import soc.storm.situation.utils.FileUtil;

/**
 * storm 由 kafka 消费流量
 * @author xudong
 *
 */

public class KafkaConsumerSpout extends BaseRichSpout {
	
	//手动指定序列化id，防止后续类修改导致反序列化失败
	private static final long serialVersionUID = -2639126860311224888L;
	
	private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerSpout.class);
	
	//kafka 集群   kerberos 认证
    static {
        System.out.println("--------------------KafkaConsumerSpout-------------SystemMapEnrichConstants.BROKER_URL:" + SystemMapEnrichConstants.BROKER_URL);
        if (SystemMapEnrichConstants.IS_KERBEROS.equals("true")) {
            System.setProperty("java.security.auth.login.config",
            		SystemMapEnrichConstants.KAFKA_KERBEROS_PATH + File.separator + "kafka_server_jaas.conf");
            System.setProperty("java.security.krb5.conf", SystemMapEnrichConstants.KAFKA_KERBEROS_PATH + File.separator + "krb5.conf");
        }
    }
    
    private final String topicInput;
    private SpoutOutputCollector spoutOutputCollector;
    private KafkaConsumerManager kafkaConsumerManager;
	
	
	public  KafkaConsumerSpout(String topicInput) {
		
		logger.info("--------------KafkaConsumerSpout init[{}]", topicInput);	
		this.topicInput = topicInput;
		
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		
		logger.info("------------------------KafkaConsumerSpout--open");
        try {
            FileUtil.testConfigFile("KafkaConsumerSpout--open");
        } catch (Exception e1) {
            e1.printStackTrace();
        }
        this.spoutOutputCollector = collector;
        this.kafkaConsumerManager = new KafkaConsumerManager(topicInput);

	}

	@Override
	public void nextTuple() {
		
		this.kafkaConsumerManager.run(spoutOutputCollector);

	}
	
    @Override
    public void ack(Object msgId) {
    	logger.info("kafka messages consumer success: {}", msgId.toString());
        // waitAck.remove(msgId);
    }

    @Override
    public void fail(Object msgId) {
        logger.warn("kafka messages consumer fails: {}", msgId.toString());
        // 重发如果不开启ackfail机制，那么spout的map对象中的该数据不会被删除的。
        // collector.emit(new Values(waitAck.get(msgId)), msgId);
    }

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
		declarer.declare(new Fields("log"));

	}

}

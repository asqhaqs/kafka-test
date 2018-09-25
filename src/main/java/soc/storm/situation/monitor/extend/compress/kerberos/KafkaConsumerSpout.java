
package soc.storm.situation.monitor.extend.compress.kerberos;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import soc.storm.situation.contants.SystemConstants;
import soc.storm.situation.utils.FileUtil;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

/**
 * KafkaConsumerSpout
 * 
 * @author wangbin03
 *
 */
public class KafkaConsumerSpout extends BaseRichSpout {
    /**
     * 
     */
    private static final long serialVersionUID = -6932165001380993216L;
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerSpout.class);

    // static {
    // System.out.println("--------------------KafkaConsumerSpout-------------SystemConstants.BROKER_URL:" +
    // SystemConstants.BROKER_URL);
    // if (SystemConstants.IS_KERBEROS.equals("true")) {
    // System.setProperty("java.security.auth.login.config",
    // SystemConstants.KAFKA_KERBEROS_PATH + File.separator + "kafka_server_jaas.conf");
    // System.setProperty("java.security.krb5.conf", SystemConstants.KAFKA_KERBEROS_PATH + File.separator +
    // "krb5.conf");
    // }
    // }

    private SpoutOutputCollector collector;
    private final String topic;

    private KafkaConsumerManager kafkaConsumerManager;

    public KafkaConsumerSpout(String topic) {
        try {
            // FileUtil.testConfigFile("KafkaConsumerSpout");
        } catch (Exception e1) {
            e1.printStackTrace();
        }// --add zhongsanmu 20180104

        logger.info("KafkaConsumerSpout init [{}]", topic);
        this.topic = topic;

    }

    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        System.out.println("---------------------------------KafkaConsumerSpout--open");

        System.setProperty("java.security.auth.login.config",
            SystemConstants.KAFKA_KERBEROS_PATH + "/kafka_server_jaas.conf");
        System.setProperty("java.security.krb5.conf", SystemConstants.KAFKA_KERBEROS_PATH + "/krb5.conf");
        try {
            FileUtil.testConfigFile("KafkaConsumerSpout");
        } catch (Exception e1) {
            e1.printStackTrace();
        }// --add zhongsanmu 20180104

        this.collector = collector;
        this.kafkaConsumerManager = new KafkaConsumerManager(topic);
    }

    @Override
    public void nextTuple() {
        kafkaConsumerManager.run(collector);
    }

    @Override
    public void close() {
    }

    @Override
    public void ack(Object msgId) {
        System.out.println("消息处理成功:" + msgId);
    }

    @Override
    public void fail(Object msgId) {
        System.out.println("消息处理失败:" + msgId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("log"));
    }

}

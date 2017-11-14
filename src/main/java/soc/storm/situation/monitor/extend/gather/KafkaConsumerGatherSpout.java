
package soc.storm.situation.monitor.extend.gather;

import java.lang.reflect.Method;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import soc.storm.situation.protocolbuffer.AddressBookProtos.SENSOR_LOG;
import soc.storm.situation.utils.TopicMethodUtil;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

/**
 * 
 * @author wangbin03
 *
 */
public class KafkaConsumerGatherSpout extends BaseRichSpout {
    /**
     * 
     */
    private static final long serialVersionUID = -6932165001380993216L;
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerGatherSpout.class);

    private SpoutOutputCollector collector;
    private final String topic;

    // EnrichmentBolt
    private String topicMethod;// = "getSkyeyeTcpflow";
    private Method getSkyeyeWebFlowLogObjectMethod;
    private final static Class<?> skyeyeWebFlowLogClass = SENSOR_LOG.class;

    private KafkaConsumerGatherManager kafkaConsumerManager;

    // KafkaProducerBolt
    private String topicNameOutput;// = "ty_tcpflow_output";

    //
    private static Jedis jedis = new Jedis("10.95.37.16", 6380);
    private long sumCount = 0;
    private int times = 0;

    public KafkaConsumerGatherSpout(String topic, String topicNameInput, String topicNameOutput) {
        logger.info("KafkaConsumerSpout init [{}]", topic);
        this.topic = topic;

        // EnrichmentBolt
        this.topicMethod = TopicMethodUtil.getTopicMethod(topicNameInput);

        // KafkaProducerBolt
        this.topicNameOutput = topicNameOutput;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        // EnrichmentBolt
        try {
            getSkyeyeWebFlowLogObjectMethod = skyeyeWebFlowLogClass.getMethod(topicMethod);
        } catch (Exception e) {
            logger.error("getSkyeyeWebFlowLogObjectMethod [{}] error", topicMethod, e);
        }

        //
        this.collector = collector;
        this.kafkaConsumerManager = new KafkaConsumerGatherManager(topic, topicMethod, getSkyeyeWebFlowLogObjectMethod, topicNameOutput);
    }

    @Override
    public void nextTuple() {
        long count = kafkaConsumerManager.execute(collector);

        sumCount += count;
        times++;

        //
        if (0 == times % 20) {
            jedis.incrBy("MessageCount", sumCount);
            sumCount = 0;
        }
    }

    @Override
    public void close() {
        //
    }

    @Override
    public void ack(Object msgId) {
        System.out.println("消息处理成功:" + msgId);
        // waitAck.remove(msgId);
    }

    @Override
    public void fail(Object msgId) {
        System.out.println("消息处理失败:" + msgId);
        // 重发如果不开启ackfail机制，那么spout的map对象中的该数据不会被删除的。
        // collector.emit(new Values(waitAck.get(msgId)), msgId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("log"));
    }

}


package soc.storm.situation.monitor.extend.compress;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class KafkaConsumerSpout extends BaseRichSpout {
    /**
     * 
     */
    private static final long serialVersionUID = -6932165001380993216L;
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerSpout.class);

    private SpoutOutputCollector collector;
    private final String topic;

    // private transient KafkaConsumerTask consumer;
    // private HashMap<String, byte[]> waitAck = new HashMap<String, byte[]>();

    private KafkaConsumerManager kafkaConsumerManager;

    public KafkaConsumerSpout(String topic) {
        logger.info("KafkaConsumerSpout init [{}]", topic);
        this.topic = topic;

    }

    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.kafkaConsumerManager = new KafkaConsumerManager(topic);
    }

    @Override
    public void nextTuple() {
        kafkaConsumerManager.run(collector);
    }

    @Override
    public void close() {
        //
        // consumer.closeKafkaConsumerTask();
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

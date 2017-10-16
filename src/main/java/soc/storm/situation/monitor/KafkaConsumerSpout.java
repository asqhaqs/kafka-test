
package soc.storm.situation.monitor;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

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
    private Queue<byte[]> queue = new ConcurrentLinkedQueue<byte[]>();

    private transient KafkaConsumerTask consumer;// = new KafkaConsumerTask(topic);

    public KafkaConsumerSpout(String topic) {
        logger.info("KafkaConsumerSpout init [{}]", topic);
        this.topic = topic;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;

        consumer = new KafkaConsumerTask(topic);
        consumer.start();
        queue = consumer.getQueue();
    }

    @Override
    public void nextTuple() {
        if (queue.size() > 0) {
            byte[] str = queue.poll();
            // collector.emit(new Values(str), UUID.randomUUID().toString());
            collector.emit(new Values(str));
            // Utils.sleep(500);
        }
    }

    @Override
    public void close() {
        //
        consumer.closeKafkaConsumerTask();
    }

    @Override
    public void ack(Object msgId) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("log"));
    }

}

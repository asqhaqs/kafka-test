
package soc.storm.situation.monitor.extend.gather;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

// import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.shade.org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import soc.storm.situation.contants.SystemConstants;
import soc.storm.situation.protocolbuffer.AddressBookProtos.SENSOR_LOG;
import soc.storm.situation.utils.Geoip;
import soc.storm.situation.utils.Geoip.Result;
import soc.storm.situation.utils.JsonUtils;
import soc.storm.situation.utils.TopicMethodUtil;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

import com.google.protobuf.Message;
import com.googlecode.protobuf.format.JsonFormat;

/**
 * @author zhongsanmu
 *
 */
public class EnrichmentTask implements Runnable {// extends BaseRichBolt
    private static final Logger logger = LoggerFactory.getLogger(EnrichmentTask.class);

    private String topicMethod;// = "getSkyeyeTcpflow";
    private Method getSkyeyeWebFlowLogObjectMethod;
    private final static Class<?> skyeyeWebFlowLogClass = SENSOR_LOG.class;

    // add zhongsanmu 20171107
    private MessageCacheProcess<byte[]> messageCacheProcess = new MessageCacheProcess<byte[]>();

    // add zhongsanmu 20171107
    private KafkaProducerTask kafkaProducerTask;
    private ExecutorService kafkaProducerTaskThreadPool;
    private static int KAFKAP_RODUCER_TASK_THREAD_TIMES = Integer.parseInt(SystemConstants.KAFKAP_RODUCER_TASK_THREAD_TIMES);

    public MessageCacheProcess<byte[]> getMessageCacheProcess() {
        return messageCacheProcess;
    }

    public EnrichmentTask(String topicNameInput, String topicNameOutput) {
        topicMethod = TopicMethodUtil.getTopicMethod(topicNameInput);

        try {
            getSkyeyeWebFlowLogObjectMethod = skyeyeWebFlowLogClass.getMethod(topicMethod);
        } catch (Exception e) {
            logger.error("getSkyeyeWebFlowLogObjectMethod [{}] error", topicMethod, e);
        }

        //
        this.kafkaProducerTask = new KafkaProducerTask(topicNameOutput);
        this.kafkaProducerTaskThreadPool = Executors.newFixedThreadPool(KAFKAP_RODUCER_TASK_THREAD_TIMES);
        for (int i = 0; i < KAFKAP_RODUCER_TASK_THREAD_TIMES; i++) {
            this.kafkaProducerTaskThreadPool.execute(kafkaProducerTask);
        }
    }

    /**
     * 
     * @param skyeyeWebFlowLogByteArray
     * @return
     */
    public Map<String, Object> enrichment(byte[] skyeyeWebFlowLogByteArray) {
        try {
            SENSOR_LOG log = SENSOR_LOG.parseFrom(skyeyeWebFlowLogByteArray);
            Object skyeyeWebFlowLogPB = getSkyeyeWebFlowLogObjectMethod.invoke(log);
            String skyeyeWebFlowLogStr = JsonFormat.printToString((Message) skyeyeWebFlowLogPB);

            // 查找ip相关的信息
            if (StringUtils.isNotBlank(skyeyeWebFlowLogStr)) {
                Map<String, Object> skyeyeWebFlowLog = JsonUtils.jsonToMap(skyeyeWebFlowLogStr);

                if (null != skyeyeWebFlowLog) {
                    // TODO:数字转换为字符串？？？？
                    convertSkyeyeWebFlowLogToStr(skyeyeWebFlowLog);

                    // （1）富化ip(sip、dip)
                    enrichmentIp(skyeyeWebFlowLog);

                    // （2）富化md5--薛杰：md5应该只涉及dns和weblog add zhongsanmu 20171031
                    enrichmentMd5(this.topicMethod, skyeyeWebFlowLog);

                    return skyeyeWebFlowLog;
                }
            } else {
                throw new RuntimeException("skyeyeWebFlowLog is not json style");
            }

        } catch (Exception e) {
            logger.error("skyeyeWebFlowLog", e);
        }

        return null;
    }

    /**
     * 转换skyeyeWebFlowLog的value为字符串类型
     * 
     * @param skyeyeWebFlowLog
     */
    private void convertSkyeyeWebFlowLogToStr(Map<String, Object> skyeyeWebFlowLog) {
        for (Entry<String, Object> entry : skyeyeWebFlowLog.entrySet()) {
            if (entry.getValue() != null) {
                try {
                    skyeyeWebFlowLog.put(entry.getKey(), entry.getValue().toString());
                } catch (Exception e) {
                    skyeyeWebFlowLog.put(entry.getKey(), "");
                }
            } else {
                skyeyeWebFlowLog.put(entry.getKey(), "");
            }
        }
    }

    /**
     * 富化ip(sip、dip)
     * 
     * @param skyeyeWebFlowLog
     * @throws Exception
     */
    private void enrichmentIp(Map<String, Object> skyeyeWebFlowLog) throws Exception {
        String sipStr = (null == skyeyeWebFlowLog.get("sip")) ? null : skyeyeWebFlowLog.get("sip").toString();
        String dipStr = (null == skyeyeWebFlowLog.get("dip")) ? null : skyeyeWebFlowLog.get("dip").toString();

        Result sipResult = Geoip.getInstance().query(sipStr);
        Result dipResult = Geoip.getInstance().query(dipStr);

        Map<String, String> sipMap = Geoip.convertResultToMap(sipResult);
        Map<String, String> dipMap = Geoip.convertResultToMap(dipResult);

        // 转换为json格式
        if (sipMap != null) {
            skyeyeWebFlowLog.put("geo_sip", sipMap);
        } else {
            skyeyeWebFlowLog.put("geo_sip", new HashMap<String, String>());
        }

        if (dipMap != null) {
            skyeyeWebFlowLog.put("geo_dip", dipMap);
        } else {
            skyeyeWebFlowLog.put("geo_dip", new HashMap<String, String>());
        }
    }

    /**
     * 富化md5
     * 
     * @param topicMethod
     * @param skyeyeWebFlowLog
     */
    private void enrichmentMd5(String topicMethod, Map<String, Object> skyeyeWebFlowLog) {
        switch (topicMethod) {
        case "getSkyeyeDns":// dns
            // host_md5
            Object hostDns = skyeyeWebFlowLog.get("host");
            String hostDnsStr = (null != hostDns) ? hostDns.toString() : "";
            skyeyeWebFlowLog.put("host_md5", DigestUtils.md5Hex(hostDnsStr).toLowerCase());
            break;
        case "getSkyeyeWeblog":// weblog
            // uri_md5
            Object uriWebLog = skyeyeWebFlowLog.get("uri");
            String uriWebLogStr = (null != uriWebLog) ? uriWebLog.toString() : "";
            skyeyeWebFlowLog.put("uri_md5", DigestUtils.md5Hex(uriWebLogStr).toLowerCase());

            // host_md5
            Object hostWebLog = skyeyeWebFlowLog.get("host");
            String hostWebLogStr = (null != hostWebLog) ? hostWebLog.toString() : "";
            skyeyeWebFlowLog.put("host_md5", DigestUtils.md5Hex(hostWebLogStr).toLowerCase());
            break;
        default:
            break;
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("message"));
    }

    @Override
    public void run() {
        try {
            // while (running && !runThread.isInterrupted()) {
            while (true) {
                //
                byte[] skyeyeWebFlowLogBytes = messageCacheProcess.getMessage();

                //
                Map<String, Object> skyeyeWebFlowLog = enrichment(skyeyeWebFlowLogBytes);
                kafkaProducerTask.getMessageCacheProcess().addMessage(skyeyeWebFlowLog);
            }
        } catch (Exception e) {
        } finally {
        }
    }

}

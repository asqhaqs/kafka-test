
package soc.storm.situation.monitor;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.protobuf.Message;
import com.googlecode.protobuf.format.JsonFormat;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import soc.storm.situation.protocolbuffer.AddressBookProtos.SENSOR_LOG;
import soc.storm.situation.utils.Geoip;
import soc.storm.situation.utils.Geoip.Result;
import soc.storm.situation.utils.JsonUtils;
import soc.storm.situation.utils.TopicMethodUtil;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author zhongsanmu
 *
 */
public class EnrichmentBolt extends BaseRichBolt {

    /**
     *
     */
    private static final long serialVersionUID = -2639126860311224615L;

    private static final Logger logger = LoggerFactory.getLogger(EnrichmentBolt.class);

    private OutputCollector outputCollector;
    private String topicMethod;// = "getSkyeyeTcpflow";
    private Method getSkyeyeWebFlowLogObjectMethod;
    private final static Class<?> skyeyeWebFlowLogClass = SENSOR_LOG.class;

    public EnrichmentBolt(String topicNameInput) {
        topicMethod = TopicMethodUtil.getTopicMethod(topicNameInput);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;

        try {
            getSkyeyeWebFlowLogObjectMethod = skyeyeWebFlowLogClass.getMethod(topicMethod);
        } catch (Exception e) {
            logger.error("getSkyeyeWebFlowLogObjectMethod [{}] error", topicMethod, e);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        // String skyeyeWebFlowLogStr002 = (String) tuple.getValue(0);
        byte[] skyeyeWebFlowLogByteArray = (byte[]) tuple.getValue(0);

        try {
            // logger.error("====" + new String(skyeyeWebFlowLogByteArray, "utf-8"));
            SENSOR_LOG log = SENSOR_LOG.parseFrom(skyeyeWebFlowLogByteArray);
            // Class<?> skyeyeWebFlowLogClass = SENSOR_LOG.class;
            // Method getSkyeyeWebFlowLogObjectMethod = skyeyeWebFlowLogClass.getMethod(topicMethod);
            Object skyeyeWebFlowLogPB = getSkyeyeWebFlowLogObjectMethod.invoke(log);
            String skyeyeWebFlowLogStr = JsonFormat.printToString((Message) skyeyeWebFlowLogPB);

            // 查找ip相关的信息
            if (StringUtils.isNotBlank(skyeyeWebFlowLogStr)) {
                // System.out.println("------------ipEnrichmentBolt:" + skyeyeWebFlowLogStr);
                // logger.error("------------ipEnrichmentBolt:" + skyeyeWebFlowLogStr);
                Map<String, Object> skyeyeWebFlowLog = JsonUtils.jsonToMap(skyeyeWebFlowLogStr);

                if (null != skyeyeWebFlowLog) {
                    // TODO:数字转换为字符串？？？？
                    convertSkyeyeWebFlowLogToStr(skyeyeWebFlowLog);

                    // （1）富化ip(sip、dip)
                    enrichmentIp(skyeyeWebFlowLog);

                    // （2）富化md5--薛杰：md5应该只涉及dns和weblog add zhongsanmu 20171031
                    enrichmentMd5(this.topicMethod, skyeyeWebFlowLog);

                    // System.out.println("skyeyeWebFlowLog: " + JsonUtils.mapToJson(skyeyeWebFlowLog));
                    this.outputCollector.emit(tuple, new Values(skyeyeWebFlowLog));
                }
            } else {
                throw new RuntimeException("skyeyeWebFlowLog is not json style");
            }

        } catch (Exception e) {
            logger.error("skyeyeWebFlowLog", e);
            // logger.error("skyeyeWebFlowLog:{}", skyeyeWebFlowLogStr, e);
            // this.outputCollector.emit(tuple, new Values(skyeyeWebFlowLogStr));
        }

        // delete zhongsanmu 20171031
        // 更新kafka中partitionManager对应的offset
        // this.outputCollector.ack(tuple);
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

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("message"));
    }

}

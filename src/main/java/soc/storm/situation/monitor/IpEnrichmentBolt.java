
package soc.storm.situation.monitor;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import soc.storm.situation.protocolbuffer.AddressBookProtos.SENSOR_LOG;
// import soc.storm.situation.protocolbuffer.AddressBookProtos.SENSOR_LOG;
import soc.storm.situation.utils.Geoip;
import soc.storm.situation.utils.Geoip.Result;
import soc.storm.situation.utils.JsonUtils;
import soc.storm.situation.utils.TopicMethodUtil;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.google.protobuf.Message;
import com.googlecode.protobuf.format.JsonFormat;

/**
 * @author zhongsanmu
 *
 */
public class IpEnrichmentBolt extends BaseBasicBolt {

    /**
     * 
     */
    private static final long serialVersionUID = -2639126860311224615L;

    private static final Logger logger = LoggerFactory.getLogger(IpEnrichmentBolt.class);

    private String topicMethod;// = "getSkyeyeTcpflow";
    private Method getSkyeyeWebFlowLogObjectMethod;

    public IpEnrichmentBolt(String topicNameInput) {
        topicMethod = TopicMethodUtil.getTopicMethod(topicNameInput);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        try {
            Class<?> skyeyeWebFlowLogClass = SENSOR_LOG.class;
            getSkyeyeWebFlowLogObjectMethod = skyeyeWebFlowLogClass.getMethod(topicMethod);
        } catch (Exception e) {
            logger.error("getSkyeyeWebFlowLogObjectMethod [{}] error", topicMethod, e);
        }
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector outputCollector) {
        // String skyeyeWebFlowLogStr = (String) tuple.getValue(0);
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
                    for (Entry<String, Object> entry2 : skyeyeWebFlowLog.entrySet()) {
                        if (entry2.getValue() != null) {
                            try {
                                skyeyeWebFlowLog.put(entry2.getKey(), entry2.getValue().toString());
                            } catch (Exception e) {
                                skyeyeWebFlowLog.put(entry2.getKey(), "");
                            }
                        } else {
                            skyeyeWebFlowLog.put(entry2.getKey(), "");
                        }
                    }

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

                    // System.out.println("skyeyeWebFlowLog: " + JsonUtils.mapToJson(skyeyeWebFlowLog));
                    outputCollector.emit(new Values(skyeyeWebFlowLog));
                }
            } else {
                throw new RuntimeException("skyeyeWebFlowLog is not json style");
            }

        } catch (Exception e) {
            logger.error("skyeyeWebFlowLog", e);
            // logger.error("skyeyeWebFlowLog:{}", skyeyeWebFlowLogStr, e);
            // this.outputCollector.emit(tuple, new Values(skyeyeWebFlowLogStr));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("message"));
    }

}

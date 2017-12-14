
package soc.storm.situation.monitor.extend.compress;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.protobuf.Message;
import com.googlecode.protobuf.format.JsonFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.shade.org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import soc.storm.situation.coder.WebFlowLogGatherMsgBinCoder;
import soc.storm.situation.coder.WebFlowLogGatherMsgCoder;
import soc.storm.situation.contants.SystemConstants;
import soc.storm.situation.encrypt.AESUtil;
import soc.storm.situation.protocolbuffer.AddressBookProtos.SENSOR_LOG;
import soc.storm.situation.utils.DateTimeUtils;
import soc.storm.situation.utils.Geoip;
import soc.storm.situation.utils.Geoip.Result;
import soc.storm.situation.utils.JsonUtils;
import soc.storm.situation.utils.TopicMethodUtil;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

// import org.apache.commons.codec.digest.DigestUtils;import org.apache.commons.lang.StringUtils;

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

    // 加密解密
    private final static boolean isWebflowLogEncrypt = (SystemConstants.WEBFLOW_LOG_ENCRYPT.equals("true")) ? true : false;
    private final static AESUtil aESUtil = new AESUtil();
    static {
        aESUtil.init_aes(SystemConstants.FILE_PATH + "/decrypt.conf");
    }
    byte[] skyeyeWebFlowLogByteArrayElementBytesDest = new byte[10000];

    //
    // （1） java Serializable
    // private final static WebFlowLogGatherMsgCoder webFlowLogGatherMsgCoder = new
    // WebFlowLogGatherMsgSerializableCoder();

    // （2）sensor protocol
    private final static WebFlowLogGatherMsgCoder webFlowLogGatherMsgCoder = new WebFlowLogGatherMsgBinCoder();

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

    public static byte[] subBytes(byte[] src, int begin, int count) {
        byte[] bs = new byte[count];
        System.arraycopy(src, begin, bs, 0, count);
        return bs;
    }

    @Override
    public void execute(Tuple tuple) {
        long begin = System.currentTimeMillis();
        // String skyeyeWebFlowLogStr002 = (String) tuple.getValue(0);
        long convertBytesBegin = System.currentTimeMillis();
        byte[] skyeyeWebFlowLogByteArray = (byte[]) tuple.getValue(0);
        long convertBytesEnd = System.currentTimeMillis();
        // System.out.println("---------------------------EnrichmentBolt, convertBytes use time: "
        // + (convertBytesEnd - convertBytesBegin) + "ms");
        // System.out.println("---------------------------EnrichmentBolt, skyeyeWebFlowLogByteArray.length: "
        // + skyeyeWebFlowLogByteArray.length);

        try {
            //
            long deCompressBegin = System.currentTimeMillis();

            // TODO:
            // skyeyeWebFlowLogByteArray = SnappyCompress.deCommpress(skyeyeWebFlowLogByteArray);
            // ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(skyeyeWebFlowLogByteArray);
            // ObjectInputStream in = new ObjectInputStream(byteArrayInputStream);
            // ArrayList<Object> pbBytesWebFlowLogList = (ArrayList<Object>) in.readObject();
            // byteArrayInputStream.close();
            // in.close();

            List<Object> pbBytesWebFlowLogList = webFlowLogGatherMsgCoder.fromWire(skyeyeWebFlowLogByteArray);

            long deCompressEnd = System.currentTimeMillis();
            // System.out.println("---------------------------EnrichmentBolt, deCompress use time: "
            // + (deCompressEnd - deCompressBegin) + "ms");

            long enrichmentBegin = System.currentTimeMillis();
            List<Map<String, Object>> skyeyeWebFlowLogList = new ArrayList<Map<String, Object>>(100);
            for (Object skyeyeWebFlowLogByteArrayElement : pbBytesWebFlowLogList) {
                byte[] skyeyeWebFlowLogByteArrayElementBytes = (byte[]) skyeyeWebFlowLogByteArrayElement;

                // 加密解密 add zhongsanmu 20171213
                if (isWebflowLogEncrypt) {
                    int decryptBytesLength = aESUtil.decrypt(skyeyeWebFlowLogByteArrayElementBytes,
                        skyeyeWebFlowLogByteArrayElementBytes.length,
                        skyeyeWebFlowLogByteArrayElementBytesDest);

                    if (decryptBytesLength < 0) {
                        throw new RuntimeException("AES decrpty error");
                    }

                    skyeyeWebFlowLogByteArrayElementBytes = subBytes(skyeyeWebFlowLogByteArrayElementBytesDest, 0, decryptBytesLength);
                }

                SENSOR_LOG log = SENSOR_LOG.parseFrom(skyeyeWebFlowLogByteArrayElementBytes);
                Object skyeyeWebFlowLogPB = getSkyeyeWebFlowLogObjectMethod.invoke(log);
                String skyeyeWebFlowLogStr = JsonFormat.printToString((Message) skyeyeWebFlowLogPB);

                System.out.println("-----------------------skyeyeWebFlowLogStr:" + skyeyeWebFlowLogStr);

                // 查找ip相关的信息
                if (StringUtils.isNotBlank(skyeyeWebFlowLogStr)) {
                    Map<String, Object> skyeyeWebFlowLog = JsonUtils.jsonToMap(skyeyeWebFlowLogStr);

                    if (null != skyeyeWebFlowLog) {
                        // TODO:数字转换为字符串？？？？
                        // convertSkyeyeWebFlowLogToStr(skyeyeWebFlowLog);

                        // （1）富化ip(sip、dip)
                        enrichmentIp(skyeyeWebFlowLog);

                        // （2）富化md5--薛杰：md5应该只涉及dns和weblog add zhongsanmu 20171031
                        enrichmentMd5(this.topicMethod, skyeyeWebFlowLog);

                        // （3）添加found_time字段--格式：yyyy-MM-dd HH:mm:ss.SSS add zhongsanmu 20171127
                        // skyeyeWebFlowLog.put("found_time", DateTimeUtils.formatNowTime());
                        skyeyeWebFlowLog.put("found_time", Long.valueOf(System.currentTimeMillis()));

                        // （4）分区字段 partition_time，小时 add zhongsanmu 20171127
                        skyeyeWebFlowLog.put("partition_time", DateTimeUtils.formatNowHourTime());

                        // （5）数据类型转换，eg:int conver to long add zhongsanmu 20171127
                        enrichmentConvertDataType(this.topicMethod, skyeyeWebFlowLog);

                        // this.outputCollector.emit(tuple, new Values(skyeyeWebFlowLog));
                        // System.out.println("skyeyeWebFlowLog: " + JsonUtils.mapToJson(skyeyeWebFlowLog));
                        skyeyeWebFlowLogList.add(skyeyeWebFlowLog);
                    }
                } else {
                    throw new RuntimeException("skyeyeWebFlowLog is not json style");
                }
            }
            long enrichmentEnd = System.currentTimeMillis();
            // System.out.println("---------------------------EnrichmentBolt, enrichment use time: "
            // + (enrichmentEnd - enrichmentBegin) + "ms, pbBytesTcpFlowList.size():" + pbBytesTcpFlowList.size());

            long emitBegin = System.currentTimeMillis();
            //
            this.outputCollector.emit(tuple, new Values(skyeyeWebFlowLogList));
            long emitEnd = System.currentTimeMillis();
            // System.out.println("---------------------------EnrichmentBolt, emit use time: "
            // + (emitEnd - emitBegin) + "ms");

            long end = System.currentTimeMillis();
            System.out
                    .println("---------------------------------------------------------------------------------EnrichmentBolt, use time: "
                            + (end - begin) + "ms");
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

    /**
     * 转化数据类型
     * 
     * @param topicMethod
     * @param skyeyeWebFlowLog
     */
    private void enrichmentConvertDataType(String topicMethod, Map<String, Object> skyeyeWebFlowLog) {
        switch (topicMethod) {
        case "getSkyeyeTcpflow":
        case "getSkyeyeUdpflow":
            // uplink_length
            Object uplinkLengthWebLog = skyeyeWebFlowLog.get("uplink_length");
            skyeyeWebFlowLog.put("uplink_length", Long.parseLong(uplinkLengthWebLog.toString()));

            // downlink_length
            Object downlinkLengthWebLog = skyeyeWebFlowLog.get("downlink_length");
            skyeyeWebFlowLog.put("downlink_length", Long.parseLong(downlinkLengthWebLog.toString()));
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

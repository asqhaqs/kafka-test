
package soc.storm.situation.monitor.extend.compress3052;

import java.io.File;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.shade.org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import soc.storm.situation.coder.WebFlowLogGatherMsgBinCoder;
import soc.storm.situation.coder.WebFlowLogGatherMsgCoder;
import soc.storm.situation.contants.SystemConstants;
import soc.storm.situation.encrypt.AESUtil;
import soc.storm.situation.encrypt.Md5Util;
import soc.storm.situation.protocolbuffer.AddressBookProtos3052.SENSOR_LOG;
import soc.storm.situation.utils.BytesHexStrTranslate;
import soc.storm.situation.utils.DateTimeUtils;
import soc.storm.situation.utils.Geoip;
import soc.storm.situation.utils.Geoip.Result;
import soc.storm.situation.utils.JsonFormatProtocolBuffer;
import soc.storm.situation.utils.JsonUtils;
import soc.storm.situation.utils.TopicMethodUtil;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.google.protobuf.Message;

/**
 * @author zhongsanmu
 *
 */
public class EnrichmentBolt extends BaseRichBolt {

    /**
     *
     */
    private static final long serialVersionUID = -2639126860311224615L;

    static {
        System.out.println("--------------------EnrichmentBolt-------------SystemConstants.BROKER_URL:" + SystemConstants.BROKER_URL);
        if (SystemConstants.IS_KERBEROS.equals("true")) {
            System.setProperty("java.security.auth.login.config",
                SystemConstants.KAFKA_KERBEROS_PATH + File.separator + "kafka_server_jaas.conf");
            System.setProperty("java.security.krb5.conf", SystemConstants.KAFKA_KERBEROS_PATH + File.separator + "krb5.conf");
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(EnrichmentBolt.class);

    private OutputCollector outputCollector;
    private String topicMethod;// = "getSkyeyeTcpflow";
    private Method getSkyeyeWebFlowLogObjectMethod;
    private final static Class<?> skyeyeWebFlowLogClass = SENSOR_LOG.class;

    // 加密解密
    private final static boolean isWebflowLogEncrypt = (SystemConstants.WEBFLOW_LOG_ENCRYPT.equals("true")) ? true : false;
    private static AESUtil aESUtil = null;
    static {
        try {
            // FileUtil.testConfigFile("EnrichmentBolt");
        } catch (Exception e1) {
            e1.printStackTrace();
        }// --add zhongsanmu 20180104

        if (isWebflowLogEncrypt) {
            aESUtil = new AESUtil();
            aESUtil.init_aes(SystemConstants.FILE_PATH + "/decrypt.conf");
        }
    }
    private byte[] skyeyeWebFlowLogByteArrayElementBytesDest = new byte[Integer.parseInt(SystemConstants.WEBFLOW_LOG_ENCRYPT_BUFFER_SIZE)];// 61858764、10000

    //
    // （1） java Serializable
    // private final static WebFlowLogGatherMsgCoder webFlowLogGatherMsgCoder = new
    // WebFlowLogGatherMsgSerializableCoder();

    // （2）sensor protocol
    private final static WebFlowLogGatherMsgCoder webFlowLogGatherMsgCoder = new WebFlowLogGatherMsgBinCoder();

    // // enrichment special property
    // private String topic;// = "ty_tcpflow_output";
    // private static Map<String, String> enrichmentSpecialPropertyTopicMap = new HashMap<String, String>();
    // private static Map<String, String> enrichmentSpecialPropertyMap = new HashMap<String, String>();
    // private boolean isEnrichmentSpecialPropertyTopic;
    // static {
    // // 初始化 enrichmentSpecialPropertyMap
    // String[] enrichmentSpecialPropertyArray = SystemConstants.ENRICHMENT_SPECIAL_PROPERTY.split(",");
    // for (String enrichmentSpecialPropertyStr : enrichmentSpecialPropertyArray) {
    // String[] enrichmentSpecialPropertySubArray = enrichmentSpecialPropertyStr.split(":");
    // enrichmentSpecialPropertyTopicMap.put(enrichmentSpecialPropertySubArray[0], null);
    // enrichmentSpecialPropertyMap.put(enrichmentSpecialPropertyStr, null);
    // }
    // }

    public EnrichmentBolt(String topicNameInput) {
        // topic = topicNameInput;
        //
        topicMethod = TopicMethodUtil.getTopicMethod(topicNameInput);

        //
        // isEnrichmentSpecialPropertyTopic = enrichmentSpecialPropertyTopicMap.containsKey(topicNameInput);
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

    /**
     * 
     * @param src
     * @param begin
     * @param count
     * @return
     */
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

            System.out.println(topicMethod + "-----------------------skyeyeWebFlowLogByteArray.length:" + skyeyeWebFlowLogByteArray.length);
            List<Object> pbBytesWebFlowLogList = webFlowLogGatherMsgCoder.fromWire(skyeyeWebFlowLogByteArray);

            long deCompressEnd = System.currentTimeMillis();
            // System.out.println("---------------------------EnrichmentBolt, deCompress use time: "
            // + (deCompressEnd - deCompressBegin) + "ms");

            long enrichmentBegin = System.currentTimeMillis();
            List<Map<String, Object>> skyeyeWebFlowLogList = new ArrayList<Map<String, Object>>(100);
            // int listSize = pbBytesWebFlowLogList.size();
            // int i = 0;
            for (Object skyeyeWebFlowLogByteArrayElement : pbBytesWebFlowLogList) {
                byte[] skyeyeWebFlowLogByteArrayElementBytes = (byte[]) skyeyeWebFlowLogByteArrayElement;

                // 加密解密 add zhongsanmu 20171213
                if (isWebflowLogEncrypt) {
                    System.out.println(topicMethod + "-----------------------isWebflowLogEncrypt:" + isWebflowLogEncrypt);
                    int decryptBytesLength = aESUtil.decrypt(skyeyeWebFlowLogByteArrayElementBytes,
                        skyeyeWebFlowLogByteArrayElementBytes.length,
                        skyeyeWebFlowLogByteArrayElementBytesDest);

                    if (decryptBytesLength < 0) {
                        throw new RuntimeException("AES decrpty error");
                    }

                    skyeyeWebFlowLogByteArrayElementBytes = subBytes(skyeyeWebFlowLogByteArrayElementBytesDest, 0, decryptBytesLength);
                }

                System.out.println(topicMethod + "-----------------------skyeyeWebFlowLogByteArrayElementBytes.length:" +
                        Integer.toHexString(skyeyeWebFlowLogByteArrayElementBytes.length)
                        + ",  skyeyeWebFlowLogByteArrayElementBytes:" + BytesHexStrTranslate
                                .bytesToHexFun3(skyeyeWebFlowLogByteArrayElementBytes));

                // try {
                // i++;
                // SENSOR_LOG log2 = SENSOR_LOG.parseFrom(skyeyeWebFlowLogByteArrayElementBytes);
                // } catch (Exception e) {
                // e.printStackTrace();
                // }

                SENSOR_LOG log = SENSOR_LOG.parseFrom(skyeyeWebFlowLogByteArrayElementBytes);
                Object skyeyeWebFlowLogPB = getSkyeyeWebFlowLogObjectMethod.invoke(log);
                // String skyeyeWebFlowLogStr = JsonFormat.printToString((Message) skyeyeWebFlowLogPB);
                String skyeyeWebFlowLogStr = JsonFormatProtocolBuffer.printToString((Message) skyeyeWebFlowLogPB);

                System.out.println(topicMethod + "enrichmentBolt-----------------------skyeyeWebFlowLogStr:" + skyeyeWebFlowLogStr);

                // 查找ip相关的信息
                if (StringUtils.isNotBlank(skyeyeWebFlowLogStr)) {
                    Map<String, Object> skyeyeWebFlowLog = JsonUtils.jsonToMap(skyeyeWebFlowLogStr);
                    // Map<String, Object> skyeyeWebFlowLog = skyeyeWebFlowLogMap;

                    if (null != skyeyeWebFlowLog) {
                        // TODO:数字转换为字符串？？？？
                        // convertSkyeyeWebFlowLogToStr(skyeyeWebFlowLog);

                        // （1）富化ip(sip、dip; Webids：victim、attacker)
                        enrichmentIp(skyeyeWebFlowLog);

                        // （2）富化md5--薛杰：md5应该只涉及dns和weblog（append file 20180716） add zhongsanmu 20171031
                        enrichmentMd5(this.topicMethod, skyeyeWebFlowLog);

                        // （3）添加found_time字段--格式：yyyy-MM-dd HH:mm:ss.SSS add zhongsanmu 20171127
                        skyeyeWebFlowLog.put("es_timestamp", Long.valueOf(System.currentTimeMillis()));
                        // 添加es_version字段 “1” add zhongsanmu 20180123
                        skyeyeWebFlowLog.put("es_version", "1");
                        // 添加event_id字段 uuid add zhongsanmu 20180123
                        skyeyeWebFlowLog.put("event_id", UUID.randomUUID().toString());

                        // （4）分区字段 hive_partition_time，小时 update zhongsanmu 20180123
                        skyeyeWebFlowLog.put("hive_partition_time", getPartitionTime());

                        // （5）数据类型转换，eg:int conver to long add zhongsanmu 20171127
                        enrichmentConvertDataType(this.topicMethod, skyeyeWebFlowLog);

                        // （6）字段名称修改：mail_from-->es_from add zhongsanmu 20180123
                        enrichmentConvertDataName(this.topicMethod, skyeyeWebFlowLog);

                        // this.outputCollector.emit(tuple, new Values(skyeyeWebFlowLog));
                        System.out.println(topicMethod + "enrichmentBolt-------skyeyeWebFlowLog: " + JsonUtils.mapToJson(skyeyeWebFlowLog));
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
            // System.out
            // .println("---------------------------------------------------------------------------------EnrichmentBolt, use time: "
            // + (end - begin) + "ms");
        } catch (Exception e) {
            logger.error("skyeyeWebFlowLog-" + topicMethod, e);
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
    @Deprecated
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
     * 富化ip(sip、dip; Webids：victim、attacker)
     * 
     * @param skyeyeWebFlowLog
     * @throws Exception
     */
    private void enrichmentIp(Map<String, Object> skyeyeWebFlowLog) throws Exception {
        // （1）sip、dip
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

        // （2）Webids：victim、attacker
        switch (topicMethod) {
        case "getSkyeyeWebshell":
        case "getSkyeyeWebattack":
        case "getSkyeyeIds":
            String victimStr = (null == skyeyeWebFlowLog.get("victim")) ? null : skyeyeWebFlowLog.get("victim").toString();
            String attackerStr = (null == skyeyeWebFlowLog.get("attacker")) ? null : skyeyeWebFlowLog.get("attacker").toString();

            Result victimResult = Geoip.getInstance().query(victimStr);
            Result attackerResult = Geoip.getInstance().query(attackerStr);

            Map<String, String> victimMap = Geoip.convertResultToMap(victimResult);
            Map<String, String> attackerMap = Geoip.convertResultToMap(attackerResult);

            // 转换为json格式
            if (victimMap != null) {
                skyeyeWebFlowLog.put("geo_victim", victimMap);
            } else {
                skyeyeWebFlowLog.put("geo_victim", new HashMap<String, String>());
            }

            if (attackerMap != null) {
                skyeyeWebFlowLog.put("geo_attacker", attackerMap);
            } else {
                skyeyeWebFlowLog.put("geo_attacker", new HashMap<String, String>());
            }
            break;
        default:
            break;
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
            skyeyeWebFlowLog.put("host_md5", Md5Util.enrichmentHostMd5Hex(hostDnsStr));
            break;
        case "getSkyeyeWeblog":// weblog
        case "getSkyeyeFile":// file
            // uri_md5
            Object uriWebLog = skyeyeWebFlowLog.get("uri");
            String uriWebLogStr = (null != uriWebLog) ? uriWebLog.toString() : "";
            skyeyeWebFlowLog.put("uri_md5", DigestUtils.md5Hex(uriWebLogStr).toLowerCase());

            // host_md5
            Object hostWebLog = skyeyeWebFlowLog.get("host");
            String hostWebLogStr = (null != hostWebLog) ? hostWebLog.toString() : "";
            skyeyeWebFlowLog.put("host_md5", Md5Util.enrichmentHostMd5Hex(hostWebLogStr));
            break;
        default:
            break;
        }
    }

    /**
     * 获取Hive分区时间
     * 
     */
    private String getPartitionTime() {
        switch (SystemConstants.HIVE_PARTITION_TIME_TYPE) {
        case "0":// 月
            return DateTimeUtils.formatNowMonthTime();
        case "1":// 天
            return DateTimeUtils.formatNowDayTime();
        case "2":// 时
            return DateTimeUtils.formatNowHourTime();
        default:// 默认：天
            return DateTimeUtils.formatNowDayTime();
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

    /**
     * 字段名称修改
     * 
     * @param topicMethod
     * @param skyeyeWebFlowLog
     */
    private void enrichmentConvertDataName(String topicMethod, Map<String, Object> skyeyeWebFlowLog) {
        // （1）mail_from-->es_from --add zhongsanmu 20180123
        switch (topicMethod) {
        case "getSkyeyeMail":
        case "getSkyeyeMailSandbox":
            // mail_from --> es_from
            Object esFrom = skyeyeWebFlowLog.get("mail_from");
            skyeyeWebFlowLog.put("es_from", esFrom);
            skyeyeWebFlowLog.remove("mail_from");
            break;
        default:
            break;
        }

        // （2）to是hive关键字，重命名为es_to --add zhongsanmu 20180124
        if (skyeyeWebFlowLog.containsKey("to")) {
            Object esTo = skyeyeWebFlowLog.get("to");
            skyeyeWebFlowLog.put("es_to", esTo);
            skyeyeWebFlowLog.remove("to");
        }

        // （3）user是hive关键字，重命名为es_user --add zhongsanmu 20180124
        if (skyeyeWebFlowLog.containsKey("user")) {
            Object esUser = skyeyeWebFlowLog.get("user");
            skyeyeWebFlowLog.put("es_user", esUser);
            skyeyeWebFlowLog.remove("user");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("message"));
    }

}

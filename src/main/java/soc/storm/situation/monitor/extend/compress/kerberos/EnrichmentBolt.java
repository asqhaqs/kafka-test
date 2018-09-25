
package soc.storm.situation.monitor.extend.compress.kerberos;

import java.lang.reflect.Method;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import soc.storm.situation.coder.WebFlowLogGatherMsgBinCoder;
import soc.storm.situation.coder.WebFlowLogGatherMsgCoder;
import soc.storm.situation.contants.SystemConstants;
import soc.storm.situation.encrypt.AESUtil;
import soc.storm.situation.protocolbuffer.AddressBookProtos.SENSOR_LOG;
import soc.storm.situation.utils.FileUtil;
import soc.storm.situation.utils.TopicMethodUtil;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

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

    // static {
    // System.out.println("--------------------EnrichmentBolt-------------SystemConstants.BROKER_URL:" +
    // SystemConstants.BROKER_URL);
    // if (SystemConstants.IS_KERBEROS.equals("true")) {
    // System.setProperty("java.security.auth.login.config",
    // SystemConstants.KAFKA_KERBEROS_PATH + File.separator + "kafka_server_jaas.conf");
    // System.setProperty("java.security.krb5.conf", SystemConstants.KAFKA_KERBEROS_PATH + File.separator +
    // "krb5.conf");
    // }
    // }

    private static final Logger logger = LoggerFactory.getLogger(EnrichmentBolt.class);

    private OutputCollector outputCollector;
    private String topicMethod;// = "getSkyeyeTcpflow";
    private Method getSkyeyeWebFlowLogObjectMethod;
    private final static Class<?> skyeyeWebFlowLogClass = SENSOR_LOG.class;

    // 加密解密
    private final static boolean isWebflowLogEncrypt = (SystemConstants.WEBFLOW_LOG_ENCRYPT.equals("true")) ? true : false;
    private static AESUtil aESUtil = null;
    static {
        // try {
        // FileUtil.testConfigFile("EnrichmentBolt");
        // } catch (Exception e1) {
        // e1.printStackTrace();
        // }// --add zhongsanmu 20180104

        if (isWebflowLogEncrypt) {
            aESUtil = new AESUtil();
            aESUtil.init_aes(SystemConstants.FILE_PATH + "/decrypt.conf");
        }
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
        System.setProperty("java.security.auth.login.config",
            SystemConstants.KAFKA_KERBEROS_PATH + "/kafka_server_jaas.conf");
        System.setProperty("java.security.krb5.conf", SystemConstants.KAFKA_KERBEROS_PATH + "/krb5.conf");
        try {
            FileUtil.testConfigFile("EnrichmentBolt");
        } catch (Exception e1) {
            e1.printStackTrace();
        }// --add zhongsanmu 20180104

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
        Object skyeyeWebFlowLogByteArray = tuple.getValue(0);
        System.out.println("--------------EnrichmentBolt.execute:" + skyeyeWebFlowLogByteArray);

        try {
            //
            this.outputCollector.emit(tuple, new Values(skyeyeWebFlowLogByteArray));
        } catch (Exception e) {
            logger.error("skyeyeWebFlowLog-" + topicMethod, e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("message"));
    }

}

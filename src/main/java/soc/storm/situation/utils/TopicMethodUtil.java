
package soc.storm.situation.utils;

import java.util.HashMap;
import java.util.Map;

import soc.storm.situation.contants.SystemConstants;

/**
 * 
 * @author zhongsanmu
 *
 */
public class TopicMethodUtil {

    private static Map<String, String> topicMethodMap = null;

    static {
        topicMethodMap = new HashMap<String, String>();
        String[] topicNameInputArray = SystemConstants.TOPIC_NAME_INPUT.split(",");
        String[] topicToMethodArray = SystemConstants.TOPIC_TO_METHOD.split(",");
        for (int i = 0; i < topicToMethodArray.length; i++) {
            topicMethodMap.put(topicNameInputArray[i], topicToMethodArray[i]);
        }

        // #topic_to_method= getSkyeyeSql, getSkyeyeDns, getSkyeyeFile, getSkyeyeFtpop, getSkyeyeLogin,
        // getSkyeyeMail,getSkyeyeLdap, getSkyeyeSsl, getSkyeyeTcpflow, getSkyeyeUdpflow,
        // getSkyeyeWeblog,getSkyeyeAbnormal

        // #topic_name_input=
        // ty_db,
        // ty_dns,
        // ty_file,
        // ty_ftpop,
        // ty_login,
        // ty_mail,
        // ty_other,
        // ty_ssl,
        // ty_tcpflow,
        // ty_udp,
        // ty_weblog,
        // ty_abnormal

        // #topic_to_method=
        // getSkyeyeSql,
        // getSkyeyeDns,
        // getSkyeyeFile,
        // getSkyeyeFtpop,
        // getSkyeyeLogin,
        // getSkyeyeMail,
        // getSkyeyeLdap,
        // getSkyeyeSsl,
        // getSkyeyeTcpflow,
        // getSkyeyeUdpflow,
        // getSkyeyeWeblog,
        // getSkyeyeAbnormal

        // optional TCPFLOW skyeye_tcpflow=2;
        // log.getSkyeyeTcpflow()
        // optional DNS skyeye_dns=3;
        // log.getSkyeyeDns()
        // optional WEBLOG skyeye_weblog=4;
        // log.getSkyeyeWeblog()
        // optional FILE_BEHAVIOR skyeye_file=5;
        // log.getSkyeyeFile()
        // optional MAIL_BEHAVIOR skyeye_mail=6;
        // log.getSkyeyeMail()
        // optional LOGIN skyeye_login=7;
        // log.getSkyeyeLogin()
        // optional DB skyeye_sql=8;
        // log.getSkyeyeSql()
        // optional ATTACK skyeye_attack=9;
        // log.getSkyeyeAttack()
        // optional LDAP skyeye_ldap=10;
        // log.getSkyeyeLdap()
        // optional SSL skyeye_ssl=11;
        // log.getSkyeyeSsl()
        // optional FTP_OP skyeye_ftpop=12;
        // log.getSkyeyeFtpop()
        // optional FILE_SANDBOX skyeye_file_sandbox=13;
        // log.getSkyeyeFileSandbox()
        // optional MAIL_SANDBOX skyeye_mail_sandbox=14;
        // log.getSkyeyeMailSandbox()
        // optional SNORT skyeye_snort=15;
        // log.getSkyeyeSnort()
        // optional UDPFLOW skyeye_udpflow=16;
        // log.getSkyeyeUdpflow()
        // optional WEBSHELL_DOLOG skyeye_webshell=17;
        // log.getSkyeyeWebshell()
        // optional WEBATTACK_DOLOG skyeye_webattack=18;
        // log.getSkyeyeAttack()
        // optional IDS_DOLOG skyeye_ids=19;
        // log.getSkyeyeIds()
        // optional ABNORMAL_PKT skyeye_abnormal=20;
        // log.getSkyeyeAbnormal()
        // optional REDIRECT skyeye_redirect=21;
        // log.getSkyeyeRedirect()
    }

    public static String getTopicMethod(String topic) {
        return topicMethodMap.get(topic);
    }

    public static void main(String[] args) {
        String topicMethod = TopicMethodUtil.getTopicMethod("ty_dns");
        System.out.println("topicMethod:" + topicMethod);
    }
}


package soc.storm.situation.contants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author wangbin03
 *
 */
public class SystemConstants extends ConfigurableContants {

    private static final Logger logger = LoggerFactory.getLogger(SystemConstants.class);

    static {
        logger.info("init");
        init("/app.properties");
    }

    public static final String ZOOKEEPER_HOSTS = getProperty("zookeeper_hosts", "127.0.0.1:2181");
    public static final String BROKER_URL = getProperty("broker_url", "127.0.0.1:9092");
    public static final String TOPOLOGY_WORKER_NUM = getProperty("topology_worker_num", "1");
    public static final String MAX_SPOUT_PENDING = getProperty("max_spout_pending", "5000");
    public static final String TOPOLOGY_NAME = getProperty("topology_name", "extend_ip_enrichment_topology");

    public static final String TOPIC_NAME_INPUT = getProperty("topic_name_input", "ty_tcpflow");
    public static final String TOPIC_NAME_OUTPUT = getProperty("topic_name_output", "ty_tcpflow_output");
    public static final String TOPIC_TO_METHOD = getProperty("topic_to_method", "getSkyeyeDns");
    public static final String FILE_PATH = getProperty("file_path", "/home/storm/geoipdata");

    public static final String KAFKA_SPOUT_THREADS = getProperty("kafka_spout_threads", "5");
    public static final String ENRICHMENT_BOLT_THREADS = getProperty("enrichment_bolt_threads", "2");
    public static final String KAFKA_BOLT_THREADS = getProperty("kafka_bolt_threads", "5");

    public static final String TOPOLOGY_DEBUG = getProperty("debug", "true");

    // zookeeper_hosts=172.24.2.155:2181,172.24.2.156:2181,172.24.2.157:2181
    //
    // broker_url=172.24.2.155:9092,172.24.2.156:9092,172.24.2.157:9092
    //
    // topology_worker_num=10
    //
    // max_spout_pending=5000
    //
    // #topology name
    // topology_name=ip_enrichment_topology
    //
    // #topic_name_input=ty_db,ty_dns,ty_file,ty_ftpop,ty_login,ty_mail,ty_other,ty_ssl,ty_tcpflow,ty_udp,ty_weblog
    // topic_name_input=ty_tcpflow
    // #TOPIC_NAME_INPUT=ty_dns
    //
    // #topic_name_output=ty_db_output,ty_dns_output,ty_file_output,ty_ftpop_output,ty_login_output,ty_mail_output,ty_other_output,ty_ssl_output,ty_tcpflow_output,ty_udp_output,ty_weblog_output
    // topic_name_output=ty_tcpflow_output
    // #TOPIC_NAME_OUTPUT =ty_dns_enrichment
    //
    // file_path=/home/storm/geoipdata
    // #file_path=E:/test
    //
    // kafka_spout_threads=5
    // ip_enrichment_bolt_threads=2
    // kafka_bolt_threads=5
    //
    // # debug
    // debug = true

}

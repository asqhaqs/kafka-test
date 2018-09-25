
package soc.storm.situation.contants;

import java.io.File;

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
        // init("/app.properties");
        init("/home/storm/geoipdata/app.properties");
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
    // TODO:
    public static final String ENRICHMENT_TASK_THREAD_TIMES = getProperty("enrichment_task_thread_times", "10");
    public static final String KAFKAP_RODUCER_TASK_THREAD_TIMES = getProperty("kafka_producer_task_thread_times", "10");

    //
    public static final String TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE = getProperty("topology_executor_receive_buffer_size", "1024");
    public static final String TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE = getProperty("topology_executor_send_buffer_size", "1024");
    public static final String TOPOLOGY_TRANSFER_BUFFER_SIZE = getProperty("kafka_producer_task_thread_times", "1024");

    // ElasticSearch
    public static final String ES_IP_ADDRESS = getProperty("es_ip_address", "10.187.101.154");
    public static final String ES_TCP_PORT = getProperty("es_tcp_port", "9300");
    public static final String ES_HTTP_PORT = getProperty("es_http_port", "9200");
    public static final String ES_CLUSTER_NAME = getProperty("es_cluster_name", "es");

    // debug
    public static final String TOPOLOGY_DEBUG = getProperty("debug", "true");

    // encrypt
    public static final String WEBFLOW_LOG_ENCRYPT = getProperty("encrypt", "false");
    public static final String WEBFLOW_LOG_ENCRYPT_BUFFER_SIZE = getProperty("encrypt_buffer_size", "100000");

    // nest record: Record--Array--Record
    public static final String RECORD_ARRAY_RECORD = getProperty("record_array_record", "skyeye_ssl:cert,skyeye_mail:attachment");

    // #webids-webattack_dolog,webids-webshell_dolog,webids-ids_dolog #victim,attacker
    // public static final String ENRICHMENT_SPECIAL_PROPERTY = getProperty(
    // "enrichment_special_property",
    // "skyeye_webshell_dolog:victim,skyeye_webattack_dolog:victim,skyeye_ids_dolog:victim,skyeye_webshell_dolog:attacker,skyeye_webattack_dolog:attacker,skyeye_ids_dolog:attacker");

    // Hive partition_time_type: 0:month 1:day 2:hour
    public static final String HIVE_PARTITION_TIME_TYPE = getProperty("hive_partition_time_type", "1");

    // Kerberos 授权&认证
    public static final String IS_KERBEROS = getProperty("is_kerberos", "true");
    public static final String KAFKA_KERBEROS_PATH = getProperty("kafka_kerberos_path", "/home/storm/geoipdata");
    static {
        if (SystemConstants.IS_KERBEROS.equals("true")) {
            System.setProperty("java.security.auth.login.config",
                SystemConstants.KAFKA_KERBEROS_PATH + File.separator + "kafka_server_jaas.conf");
            System.setProperty("java.security.krb5.conf", SystemConstants.KAFKA_KERBEROS_PATH + File.separator + "krb5.conf");
        }
    }

    // # kafka consumer group id
    public static final String KAFKA_CONSUMER_GROUP_ID_3052 = getProperty("kafka_consumer_group_id_3052", "3052");
    public static final String KAFKA_CONSUMER_GROUP_ID_3061 = getProperty("kafka_consumer_group_id_3061", "3061");
    public static final String KAFKA_CONSUMER_GROUP_ID_GYWA3061 = getProperty("kafka_consumer_group_id_gywa3061", "gywa3061");

    // # auto.offset.reset String must be one of: latest, earliest
    public static final String KAFKA_CONSUMER_AUTO_OFFSET_RESET_3052 = getProperty("kafka_consumer_auto_offset_reset_3052", "earliest");
    public static final String KAFKA_CONSUMER_AUTO_OFFSET_RESET_3061 = getProperty("kafka_consumer_auto_offset_reset_3061", "earliest");
    public static final String KAFKA_CONSUMER_AUTO_OFFSET_RESET_GYWA3061 = getProperty("kafka_consumer_auto_offset_reset_gywa3061",
        "earliest");

    public static void main(String[] args) {
        System.out.println("SystemConstants.KAFKA_SPOUT_THREADS: " + SystemConstants.KAFKA_SPOUT_THREADS);

        System.out.println("SystemConstants.KAFKA_CONSUMER_AUTO_OFFSET_RESET_GYWA3061: " +
                SystemConstants.KAFKA_CONSUMER_AUTO_OFFSET_RESET_GYWA3061);
    }

}

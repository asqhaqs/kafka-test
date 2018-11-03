package soc.storm.situation.contants;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 系统配置
 * @author xudong
 *
 */
public class SystemMapEnrichConstants extends ConfigurableContants {
	
	private static final Logger logger = LoggerFactory.getLogger(SystemMapEnrichConstants.class); 

	static {
		logger.info("init");
		init("/home/storm/geoipdata/app.properties");
	}
	
	//连接服务设置
    public static final String ZOOKEEPER_HOSTS = getProperty("zookeeper_hosts", "127.0.0.1:2181");
    public static final String BROKER_URL = getProperty("broker_url", "127.0.0.1:9092");
    public static final String TOPOLOGY_WORKER_NUM = getProperty("topology_worker_num", "1");
    public static final String MAX_SPOUT_PENDING = getProperty("max_spout_pending", "5000");
    public static final String TOPOLOGY_NAME = getProperty("topology_name", "extend_map_enrichment_topology");
    
    //线程数设置
    public static final String KAFKA_SPOUT_THREADS = getProperty("kafka_spout_threads", "5");
    public static final String ANALYSIS_BOLT_THREADS = getProperty("analysis_bolt_threads", "2");
    public static final String MAPPING_ENRICHMENT_BOLT_THREADS = getProperty("mapping_enrichment_bolt_threads", "5");
    public static final String KAFKA_BOLT_THREADS = getProperty("kafka_bolt_threads", "5");
    
    //缓冲区 设置 （待修改）
    public static final String TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE = getProperty("topology_executor_receive_buffer_size", "1024");
    public static final String TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE = getProperty("topology_executor_send_buffer_size", "1024");
    public static final String TOPOLOGY_TRANSFER_BUFFER_SIZE = getProperty("kafka_producer_task_thread_times", "1024");
    
    // debug
    public static final String TOPOLOGY_DEBUG = getProperty("debug", "true");
    
    // # kafka consumer group id
    public static final String KAFKA_CONSUMER_GROUP_ID_3052 = getProperty("kafka_consumer_group_id_3052", "3052");
    
    // # auto.offset.reset String must be one of: latest, earliest
    public static final String KAFKA_CONSUMER_AUTO_OFFSET_RESET_3052 = getProperty("kafka_consumer_auto_offset_reset_3052", "earliest");
    public static final String KAFKA_CONSUMER_AUTO_OFFSET_RESET_3061 = getProperty("kafka_consumer_auto_offset_reset_3061", "earliest");
    public static final String KAFKA_CONSUMER_AUTO_OFFSET_RESET_GYWA3061 = getProperty("kafka_consumer_auto_offset_reset_gywa3061",
        "earliest");
    
    // Hive partition_time_type: 0:month 1:day 2:hour
    public static final String HIVE_PARTITION_TIME_TYPE = getProperty("hive_partition_time_type", "1");
    
    //输入输出topic设置
    public static final String TOPIC_MAP_ENRICH_INPUT = getProperty("topic_map_enrich_input", "blend_input1");
    public static final String TOPIC_MAP_ENRICH_OUTPUT = getProperty("topic_map_enrich_output", "bde_dns_output");
    //public static final String TOPIC_TO_METHOD = getProperty("topic_to_method", "getSkyeyeDns");
    public static final String FILE_PATH = getProperty("file_path", "/home/storm/geoipdata");
    public static final String FLOW_TYPES = getProperty("flow_types","bde_dns");
    
    //数据类型  与 输出topic 映射规则  必配项！
    public static final String TYPE_MAPPING_RULES = getProperty("topic_output_mapping_flow_types","0,0,1,2,3,4,4,5,6");
    
    //jj fields map to 360 fields
//    public static final String JJCONN_TO_TCP = getProperty("jjconn_to_tcp", "src_ip,src_port,dst_ip,dst_port,app_proto,orig_12_addr,resp_12_addr,timestamp");
//    public static final String QATCP_FROM_CONN = getProperty("qatcp_from_conn", "sip,sport,dip,dport,proto,src_mac,dst_mac,access_time");
//    public static final String JJCONN_TO_UDP = getProperty("jjconn_to_udp", "src_ip,src_port,dst_ip,dst_port,orig_12_addr,resp_12_addr,proto,timestamp");
//    public static final String QAUDP_FROM_CONN = getProperty("qaudp_from_conn", "sip,sport,dip,dport,src_mac,dst_mac,proto,access_time");
//    public static final String JJSSL_TO_SSL = getProperty("jjssl_to_ssl", "src_ip,src_port,dst_ip,dst_port,version,uid,server_name,sensor_ip,timestamp");
//    public static final String QASSL_FROM_SSL = getProperty("qassl_from_ssl", "sip,sport,dip,dport,version,session_id,server_name,device_ip,access_time");
//    public static final String JJHTTP_TO_WEBLOG = getProperty("jjhttp_to_weblog", "src_ip,src_port,dst_ip,dst_port,timestamp");
//    public static final String QAWEBLOG_FROM_HTTP = getProperty("qaweblog_from_http", "sip,sport,dip,dport,access_time");
//    public static final String JJDNS_TO_DNS = getProperty("jjdns_to_dns", "src_ip,src_port,dst_ip,dst_port,rcode,addl,sensor_ip,timestamp");
//    public static final String QADNS_FROM_DNS = getProperty("qadns_from_dns", "sip,sport,dip,dport,reply_code,txt,device_ip,access_time");
//    public static final String JJFTP_TO_FILE = getProperty("jjftp_to_file", "src_ip,src_port,dst_ip,dst_port,reply_code,sensor_ip,timestamp");
//    public static final String QAFILE_FROM_FTP = getProperty("qafile_from_ftp", "sip,sport,dip,dport,status,device_ip,access_time");
//    public static final String JJFTP_TO_FTPOP = getProperty("jjftp_to_ftpop", "src_ip,src_port,dst_ip,dst_port,user,command,reply_msg,sensor_ip,timestamp");
//    public static final String QAFTPOP_FROM_FTP = getProperty("qaftpop_from_ftp", "sip,sport,dip,dport,user,op,ret,device_ip,access_time");
//    public static final String JJSMTP_TO_MAIL = getProperty("jjsmtp_to_mail", "src_ip,src_port,dst_ip,dst_port,msg_id,date,mailfrom,rcptto,cc,subject,reply_to,sensor_ip,timestamp");
//    public static final String QAMAIL_FROM_SMTP = getProperty("qamail_from_smtp", "sip,sport,dip,dport,mid,time,mail_from,to,cc,subject,returnpath,device_ip,access_time");
//    public static final String JJMYSQL_TO_SQL = getProperty("jjmysql_to_sql", "src_ip,src_port,dst_ip,dst_port,success,command,response,timestamp");
//    public static final String QASQL_FROM_MYSQL = getProperty("qasql_from_mysql", "sip,sport,dip,dport,ret_code,sql_info,normal_ret,access_time");
    public static final String JJCONN_TO_TCP = getProperty("jjconn_to_tcp", "src_ip:sip,src_port:sport,dst_ip:dip,dst_port:dport,app_proto:proto,orig_ip_bytes:uplink_length,resp_ip_bytes:downlink_length,orig_mac:src_mac,resp_mac:dst_mac,orig_pkts:uplink_pkts,resp_pkts:downlink_pkts,timestamp:access_time");
    public static final String JJCONN_TO_UDP = getProperty("jjconn_to_udp", "src_ip:sip,src_port:sport,dst_ip:dip,dst_port:dport,app_proto:proto,orig_ip_bytes:uplink_length,resp_ip_bytes:downlink_length,orig_pkts:uplink_pkts,resp_pkts:downlink_pkts,orig_mac:src_mac,resp_mac:dst_mac,timestamp:access_time");
    public static final String JJSSL_TO_SSL = getProperty("jjssl_to_ssl", "src_ip:sip,src_port:sport,dst_ip:dip,dst_port:dport,version:version,uid:session_id,server_name:server_name,sensor_ip:device_ip,timestamp:access_time");
    public static final String JJHTTP_TO_WEBLOG = getProperty("jjhttp_to_weblog", "src_ip:sip,src_port:sport,dst_ip:dip,dst_port:dport,uri:uri,method:method,timestamp:access_time");
    public static final String JJDNS_TO_DNS = getProperty("jjdns_to_dns", "src_ip:sip,src_port:sport,dst_ip:dip,dst_port:dport,rcode:reply_code,addl:txt,sensor_ip:device_ip,timestamp:access_time");
    public static final String JJFTP_TO_FILE = getProperty("jjftp_to_file", "src_ip:sip,src_port:sport,dst_ip:dip,dst_port:dport,reply_code:status,sensor_ip:device_ip,timestamp:access_time");
    public static final String JJFTP_TO_FTPOP = getProperty("jjftp_to_ftpop", "src_ip:sip,src_port:sport,dst_ip:dip,dst_port:dport,user:user,command:op,reply_msg:ret,sensor_ip:device_ip,timestamp:access_time");
    public static final String JJSMTP_TO_MAIL = getProperty("jjsmtp_to_mail", "src_ip:sip,src_port:sport,dst_ip:dip,dst_port:dport,msg_id:mid,date:time,mailfrom:mail_from,rcptto:to,cc:cc,subject:subject,reply_to:returnpath,sensor_ip:device_ip,timestamp:access_time");
    public static final String JJMYSQL_TO_SQL = getProperty("jjmysql_to_sql", "src_ip:sip,src_port:sport,dst_ip:dip,dst_port:dport,success:ret_code,cmd:sql_info,response:normal_ret,sensor_ip:device_ip,timestamp:access_time");
    
    
    //360 unmapped fields
    public static final String TCPFLOW_UNMAPPED_FIELDS = getProperty("skyeye_tcpflow_unmapped_fields","status,vendor_id,down_payload,dtime,client_os,dipv6,stime,device_ip,app_type,summary,up_payload,serial_num,sipv6,server_os,mpls_label");
    public static final String UDPFLOW_UNMAPPED_FIELDS = getProperty("skyeye_udpflow_unmapped_fields","serial_num,app_type,down_payload,device_ip,dtime,vendor_id,stime,dipv6,up_payload,sipv6,mpls_label");
    public static final String WEBLOG_UNMAPPED_FIELDS = getProperty("skyeye_weblog_unmapped_fields","origin,status,device_ip,uri_md5,accept_language,vendor_id,agent,xff,dipv6,host_md5,cookie,content_type,host,data,setcookie,serial_num,sipv6,mpls_label,referer,urlcategory");
    public static final String FILE_UNMAPPED_FIELDS = getProperty("skyeye_file_unmapped_fields","dipv6,serial_num,file_dir,proto,filename,trans_mode,method,mime_type,status,uri_md5,vendor_id,file_md5,host,referer,host_md5,uri,sipv6,mpls_label");
    public static final String FTPOP_UNMAPPED_FIELDS = getProperty("skyeye_ftpop_unmapped_fields","seq,proto,vendor_id,serial_num,sipv6,dipv6,mpls_label");
    public static final String DNS_UNMAPPED_FIELDS = getProperty("skyeye_dns_unmapped_fields","dipv6,addr,serial_num,dns_type,vendor_id,count,host_md5,host,sipv6,cname,mpls_label,mx");
    public static final String SSL_UNMAPPED_FIELDS = getProperty("skyeye_ssl_unmapped_fields","");
    public static final String MAIL_UNMAPPED_FIELDS = getProperty("skyeye_mail_unmapped_fields","dipv6,references,proto,serial_num,attachment,vendor_id,received,plain,bcc,sipv6,mpls_label");
    public static final String SQL_UNMAPPED_FIELDS = getProperty("skyeye_sql_unmapped_fields","dipv6,proto,es_user,serial_num,version,vendor_id,db_name,db_type,sipv6,mpls_label");
    
    //jinjing flow log separator
    public static final String FLOW_LOG_SEPARATOR = getProperty("flow_log_separator", "metadata - - -");
    
    //jinjing flow fields name
    public static final String CONN_FIELDS = getProperty("jinjing_conn_fields","timestamp,sensor_name,sensor_ip,event_source,interface,ts,uid,src_ip,src_port,dst_ip,dst_port");
    public static final String SSL_FIELDS = getProperty("jinjing_ssl_fields","timestamp,sensor_name,sensor_ip,event_source,interface,ts,uid,src_ip,src_port,dst_ip,dst_port");
    public static final String HTTP_FIELDS = getProperty("jinjing_http_fields","timestamp,sensor_name,sensor_ip,event_source,interface,ts,uid,src_ip,src_port,dst_ip,dst_port");
    public static final String DNS_FIELDS = getProperty("jinjing_dns_fields","timestamp,sensor_name,sensor_ip,event_source,interface,ts,uid,src_ip,src_port,dst_ip,dst_port");
    public static final String FTP_FIELDS = getProperty("jinjing_ftp_fields","timestamp,sensor_name,sensor_ip,event_source,interface,ts,uid,src_ip,src_port,dst_ip,dst_port");
    public static final String SMTP_FIELDS = getProperty("jinjing_smtp_fields","timestamp,sensor_name,sensor_ip,event_source,interface,ts,uid,src_ip,src_port,dst_ip,dst_port");
    public static final String MYSQL_FIELDS = getProperty("jinjing_mysql_fields","timestamp,sensor_name,sensor_ip,event_source,interface,ts,uid,src_ip,src_port,dst_ip,dst_port");
    
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
    
    
	public static void main(String[] args) {
		
		System.out.println("SystemMapEnrichConstants.KAFKA_SPOUT_THREADS: " + SystemMapEnrichConstants.KAFKA_SPOUT_THREADS);

	}

}

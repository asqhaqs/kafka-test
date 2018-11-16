package soc.storm.situation.monitor.extend.gatherdalian;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import soc.storm.situation.contants.SystemMapEnrichConstants;
import soc.storm.situation.utils.DateTimeUtils;
import soc.storm.situation.utils.Geoip;
import soc.storm.situation.utils.Geoip.Result;

/**
 * @Descriptioin 映射与富化
 * @author xudong
 * @Date 2018-10-31
 *
 */

public class MappingAndEnrichmentBolt extends BaseRichBolt {
	
	//手动指定序列化id，防止后续类修改导致反序列化失败
	private static final long serialVersionUID = -2639126860311224111L;
	private static final Logger logger = LoggerFactory.getLogger(MappingAndEnrichmentBolt.class);
	
	private final String topicOutput;
	private OutputCollector outputCollector;
	
    static {
        System.out.println("--------------------MappingAndEnrichmentBolt-------------SystemMapEnrichConstants.BROKER_URL:" + SystemMapEnrichConstants.BROKER_URL);
        if (SystemMapEnrichConstants.IS_KERBEROS.equals("true")) {
            System.setProperty("java.security.auth.login.config",
            		SystemMapEnrichConstants.KAFKA_KERBEROS_PATH + File.separator + "kafka_server_jaas.conf");
            System.setProperty("java.security.krb5.conf", SystemMapEnrichConstants.KAFKA_KERBEROS_PATH + File.separator + "krb5.conf");
        }
    }
	
	//金睛流量数据的字段名称列表
	private static String[] connFields;
	private static String[] httpFields;
	private static String[] dnsFields;
	private static String[] ftpFields;
	private static String[] sslFields;
	private static String[] smtpFields;
	private static String[] mysqlFields;
	//金睛告警数据字段名称列表
	private static String[] alertFields;
	//360告警字段（不在金睛告警字段中的）
	private static String[] qhAlertFields;
	
	//金睛流量字段映射规则
	private static Map<String, String> jjconnToTcp;
	private static Map<String, String> jjconnToUdp;
	private static Map<String, String> jjsslToSsl;
	private static Map<String, String> jjhttpToWeblog;
	private static Map<String, String> jjdnsToDns;
	private static Map<String, String> jjftpToFile;
	private static Map<String, String> jjftpToFtpop;
	private static Map<String, String> jjsmtpToMail;
	private static Map<String, String> jjmysqlToSql;
	
	//360流量未映射的字段
	private static String[] skyeyeTcpUnmapped;
	private static String[] skyeyeUdpUnmapped;
	private static String[] skyeyeDnsUnmapped;
	private static String[] skyeyeWeblogUnmapped;
	private static String[] skyeyeFileUnmapped;
	private static String[] skyeyeFtpopUnmapped;
	private static String[] skyeyeSslUnmapped;
	private static String[] skyeyeMailUnmapped;
	private static String[] skyeyeSqlUnmapped;
	
	
	static {
		connFields = SystemMapEnrichConstants.CONN_FIELDS.split(",");
		httpFields = SystemMapEnrichConstants.HTTP_FIELDS.split(",");
		dnsFields = SystemMapEnrichConstants.DNS_FIELDS.split(",");
		ftpFields = SystemMapEnrichConstants.FTP_FIELDS.split(",");
		sslFields = SystemMapEnrichConstants.SSL_FIELDS.split(",");
		smtpFields = SystemMapEnrichConstants.SMTP_FIELDS.split(",");
		mysqlFields = SystemMapEnrichConstants.MYSQL_FIELDS.split(",");

		alertFields = SystemMapEnrichConstants.ALERT_FIELDS.split(",");
		qhAlertFields = SystemMapEnrichConstants.QH_ALERT_FIELDS_NOT_IN_JJ.split(",");
		

		jjconnToTcp = listToMap(SystemMapEnrichConstants.JJCONN_TO_TCP);
		jjconnToUdp = listToMap(SystemMapEnrichConstants.JJCONN_TO_UDP);
		jjsslToSsl = listToMap(SystemMapEnrichConstants.JJSSL_TO_SSL);
		jjhttpToWeblog = listToMap(SystemMapEnrichConstants.JJHTTP_TO_WEBLOG);
		jjdnsToDns = listToMap(SystemMapEnrichConstants.JJDNS_TO_DNS);
		jjftpToFile = listToMap(SystemMapEnrichConstants.JJFTP_TO_FILE);
		jjftpToFtpop = listToMap(SystemMapEnrichConstants.JJFTP_TO_FTPOP);
		jjsmtpToMail = listToMap(SystemMapEnrichConstants.JJSMTP_TO_MAIL);
		jjmysqlToSql = listToMap(SystemMapEnrichConstants.JJMYSQL_TO_SQL);
		
		skyeyeTcpUnmapped = SystemMapEnrichConstants.TCPFLOW_UNMAPPED_FIELDS.split(",");
		skyeyeUdpUnmapped = SystemMapEnrichConstants.UDPFLOW_UNMAPPED_FIELDS.split(",");
		skyeyeDnsUnmapped = SystemMapEnrichConstants.DNS_UNMAPPED_FIELDS.split(",");
		skyeyeFileUnmapped = SystemMapEnrichConstants.FILE_UNMAPPED_FIELDS.split(",");
		skyeyeFtpopUnmapped = SystemMapEnrichConstants.FTPOP_UNMAPPED_FIELDS.split(",");
		skyeyeWeblogUnmapped = SystemMapEnrichConstants.WEBLOG_UNMAPPED_FIELDS.split(",");
		skyeyeSslUnmapped = SystemMapEnrichConstants.SSL_UNMAPPED_FIELDS.split(",");
		skyeyeSqlUnmapped = SystemMapEnrichConstants.SQL_UNMAPPED_FIELDS.split(",");
		skyeyeMailUnmapped = SystemMapEnrichConstants.MAIL_UNMAPPED_FIELDS.split(",");
	}
	
	
	private static Map<String, String> listToMap(String mapRelation) {
		Map<String, String> jjToSkyeyeMap = new HashMap<String, String>();
		if(StringUtils.isNotBlank(mapRelation)) {
			String[] list = mapRelation.split(",");
			for(String relation : list) {
				String key = relation.split(":")[0];
				String value = relation.split(":")[1];
				jjToSkyeyeMap.put(key, value);
			}
		}
		return jjToSkyeyeMap;
	}
	
	
	public MappingAndEnrichmentBolt(String topicOutput) {
		this.topicOutput = topicOutput;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		
		this.outputCollector = collector;
	}

	// 将 日志 中的数据 导入至 map中
	private void fillToMap(Map<String,Object> map, String[] fields, List<String> logValues) 
			throws NullPointerException,ArrayIndexOutOfBoundsException{
		//对syslog 内容数组而不是字段数组进行遍历，防止因为数据过长截断而导致的数组越界问题
		for(int i = 0; i < logValues.size(); i++) {
			map.put(fields[i].trim(), logValues.get(i).trim());
		}
	}
	
	// 将skyeye未映射的字段设置为空， 导入map中
	private void fillSkyeyeFiledsToMap(Map<String,Object> map, String[] skyeyeUnmappedFields) {
		if(skyeyeUnmappedFields != null && skyeyeUnmappedFields.length > 0) {
			for(String unmappedField : skyeyeUnmappedFields) {
				if(StringUtils.isNotBlank(unmappedField))
				logger.info("+++++++++++++++++++++++unmapped fields " + unmappedField);
//				if(unmappedField.equals("ret")) {
//					String[] ss = new String[]();
//					map.put(unmappedField, new String[]);
//				}
				map.put(unmappedField, null);
				
			}
		}
	}
	

    private void convertDataName(Map<String, String> relationMap, Map<String, Object> syslogMap) {
    	if(relationMap != null && relationMap.size() > 0){
    		for(Map.Entry<String, String> entry : relationMap.entrySet()) {
    			if(!entry.getKey().equals(entry.getValue())) {
    				logger.info("+++++++++++++++entry key " + entry.getKey());
    				logger.info("+++++++++++++++entry value " + entry.getValue());
        			String jjField = entry.getKey();
        			String skyeyeField = entry.getValue();
        			Object object = syslogMap.get(jjField);
        			syslogMap.put(skyeyeField, object);
        			syslogMap.remove(jjField);
    			}

    		}
    	}
    }

    /**
     *  map字段填充 & 字段名称修改 （映射）流量数据
     */
    private void fillToMapAndConvertDataName(String type, String topicOutput, Map<String, Object> syslogMap,
    		List<String> logValues) {
    	
    	//之所以判断topicOutput是为了使用不同的映射
    	if(type.equals("bde_conn") && topicOutput.equals("skyeye_tcpflow")) {
    		fillToMap(syslogMap, connFields, logValues);
    		convertDataName(jjconnToTcp, syslogMap);
    		
    		//金睛 conn中 proto字段与 skyeyeudpflow 中 proto 命名冲突，修改为 jj_proto
    	    Object proto = syslogMap.get("proto");
    	    syslogMap.put("jj_proto", proto);
    	    syslogMap.remove("proto");
    	    
    		fillSkyeyeFiledsToMap(syslogMap, skyeyeTcpUnmapped);
    	}else if(type.equals("bde_conn") && topicOutput.equals("skyeye_udpflow")) {
    		fillToMap(syslogMap, connFields, logValues);
    		convertDataName(jjconnToUdp, syslogMap);
    		
    		//金睛 conn中 proto字段与 skyeyeudpflow 中 proto 命名冲突，修改为 jj_proto
    	    Object proto = syslogMap.get("proto");
    	    syslogMap.put("jj_proto", proto);
    	    syslogMap.remove("proto");
    	    
    		fillSkyeyeFiledsToMap(syslogMap, skyeyeUdpUnmapped);
    	}else if(type.equals("bde_ssl") && topicOutput.equals("skyeye_ssl")) {
    		fillToMap(syslogMap, sslFields, logValues);
    		convertDataName(jjsslToSsl, syslogMap);
    		fillSkyeyeFiledsToMap(syslogMap, skyeyeSslUnmapped);
    	}else if(type.equals("bde_http") && topicOutput.equals("skyeye_weblog")) {
    		fillToMap(syslogMap, httpFields, logValues);
    		convertDataName(jjhttpToWeblog, syslogMap);
    		fillSkyeyeFiledsToMap(syslogMap, skyeyeWeblogUnmapped);
    	}else if(type.equals("bde_dns") && topicOutput.equals("skyeye_dns")) {
    		fillToMap(syslogMap, dnsFields, logValues);
    		convertDataName(jjdnsToDns, syslogMap);
    		fillSkyeyeFiledsToMap(syslogMap, skyeyeDnsUnmapped);
    	}else if(type.equals("bde_ftp") && topicOutput.equals("skyeye_file")) {
    		fillToMap(syslogMap, ftpFields, logValues);
    		convertDataName(jjftpToFile, syslogMap);
    		fillSkyeyeFiledsToMap(syslogMap, skyeyeFileUnmapped);
    	}else if(type.equals("bde_ftp") && topicOutput.equals("skyeye_ftp_op")) {
    		fillToMap(syslogMap, ftpFields, logValues);
    		convertDataName(jjftpToFtpop, syslogMap);
    		fillSkyeyeFiledsToMap(syslogMap, skyeyeFtpopUnmapped);
            if (syslogMap.containsKey("user")) {
                Object esUser = syslogMap.get("user");
                syslogMap.put("es_user", esUser);
                syslogMap.remove("user");
            }
    	}else if(type.equals("bde_smtp") && topicOutput.equals("skyeye_mail")) {
    		fillToMap(syslogMap, smtpFields, logValues);
    		convertDataName(jjsmtpToMail, syslogMap);
    		
    		//金睛 smtp 中 to字段与 
//    	    Object to = syslogMap.get("to");
//    	    syslogMap.put("jj_to", to);
//    	    syslogMap.remove("to");
    	    
    		fillSkyeyeFiledsToMap(syslogMap, skyeyeMailUnmapped);
    	}else if(type.equals("bde_mysql") && topicOutput.equals("skyeye_sql")) {
    		fillToMap(syslogMap, mysqlFields, logValues);
    		convertDataName(jjmysqlToSql, syslogMap);
    		fillSkyeyeFiledsToMap(syslogMap, skyeyeSqlUnmapped);
    	}

    }
	
	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple input) {
		
		try {
			long enrichbegin = System.currentTimeMillis();
			//得到 该syslog 的类型名
			String type = (String)input.getValue(0);
			//得到 该syslog 内容
			List<String> syslogValues = (List<String>)input.getValue(1);
			//得到 该syslog 是否是告警的标志位
			Boolean isAlert = (Boolean)input.getValue(2);
			
			Map<String, Object> syslogMap = new HashMap<String,Object>();
			
			if(!isAlert && StringUtils.isNotBlank(type) && syslogValues != null && syslogValues.size() > 0) {

				logger.info("======================== syslog is flow data, and isAlert : {}", isAlert);
				fillToMapAndConvertDataName(type, topicOutput, syslogMap, syslogValues);
				//(1) ip 富化(sip, dip)
				enrichmentIp(syslogMap, isAlert);
				//(2) md5 富化--------由于数据源（dns http）没有  host 字段所以不用做
				
				//字段类型转型
				enrichmentConvertDataType(topicOutput,syslogMap);
				
				//(3)  添加found_time字段--格式：yyyy-MM-dd HH:mm:ss.SSS
				syslogMap.put("es_timestamp", Long.valueOf(System.currentTimeMillis()));
				//(4) 添加es_version字段 “1” 
				syslogMap.put("es_version", "1");
				//(5) 添加event_id字段 uuid
				syslogMap.put("event_id", UUID.randomUUID().toString());
				//(6) 分区字段 hive_partition_time，小时 update
				syslogMap.put("hive_partition_time", getPartitionTime());
				
				// ip 富化过程中的 字段名称修改（以前的）
				if(type.equals("bde_smtp") && topicOutput.equals("skyeye_mail")) {
		            // mail_from --> es_from
		            Object esFrom = syslogMap.get("mail_from");
		            syslogMap.put("es_from", esFrom);
		            syslogMap.remove("mail_from");
		            
		            // （2）to是hive关键字，重命名为es_to --add zhongsanmu 20180124
		            if (syslogMap.containsKey("to")) {
		                Object esTo = syslogMap.get("to");
		                syslogMap.put("es_to", esTo);
		                syslogMap.remove("to");
		            }

		            // （3）user是hive关键字，重命名为es_user --add zhongsanmu 20180124
		            if (syslogMap.containsKey("user")) {
		                Object esUser = syslogMap.get("user");
		                syslogMap.put("es_user", esUser);
		                syslogMap.remove("user");
		            }
				}
				
				long enrichend = System.currentTimeMillis();
				logger.info("the mapAndEnrich time is: {}ms", enrichend-enrichbegin);
				long emitbegin = System.currentTimeMillis();
				outputCollector.emit(input,new Values(syslogMap, isAlert));
				long emitend = System.currentTimeMillis();
				logger.info("the emit time is: {}ms", emitend-emitbegin);
				
			}else if(isAlert && syslogValues != null && syslogValues.size() > 0){   //告警数据处理

				logger.info("======================== syslog is alert data, and isAlert : {}", isAlert);
				//导入map中
				fillToMap(syslogMap, alertFields, syslogValues);
				//加入 360 字段，全部置空
				for(String qhField : qhAlertFields){
					syslogMap.put(qhField, null);
				}
				//使用 金睛的sip 和 dip 进行 ip 富化
				enrichmentIp(syslogMap, isAlert);
				//添加公共头使得该条告警通过规则引擎
				syslogMap.put("found_time", DateTimeUtils.timestampToDate(syslogMap.get("timestamp").toString(),
						"yyyy-MM-dd'T'HH:mm:ss.SSSZ"));
				syslogMap.put("event_type", "005");
				syslogMap.put("event_subtype", "005100");
				syslogMap.put("industry_id", 0);
				syslogMap.put("vendor", "jinjing");
				syslogMap.put("system_id", 0);
				syslogMap.put("sip", syslogMap.get("src_ip").toString());
				syslogMap.put("dip", syslogMap.get("dst_ip").toString());
				syslogMap.put("organization_id",0);

				outputCollector.emit(input,new Values(syslogMap, isAlert));
			}
			
		}catch(ClassCastException e) {
			logger.error("the tuple value is error! ClassCastException: {}", e.getMessage(), e);
		}catch(NullPointerException e) {
			logger.error("NullPointerException: {}", e.getMessage(), e);
		}catch(Exception e) {
			logger.error("Exception: {}", e.getMessage(), e);
		}


	}
	
    /**
     * 富化ip(sip、dip; Webids：victim、attacker)
     *
     */
    private void enrichmentIp(Map<String, Object> syslogMap, Boolean isAleart) throws Exception {
		String sipStr = null;
		String dipStr = null;
    	if(!isAleart){
			// （1）sip、dip
			sipStr = (null == syslogMap.get("sip")) ? null : (String)syslogMap.get("sip");
			dipStr = (null == syslogMap.get("dip")) ? null : (String)syslogMap.get("dip");
		}else {
    		sipStr = (null == syslogMap.get("sip")) ? null : (String)syslogMap.get("src_ip");
			dipStr = (null == syslogMap.get("dip")) ? null : (String)syslogMap.get("dst_ip");
		}

        Result sipResult = Geoip.getInstance().query(sipStr);
        Result dipResult = Geoip.getInstance().query(dipStr);

        Map<String, String> sipMap = Geoip.convertResultToMap(sipResult);
        Map<String, String> dipMap = Geoip.convertResultToMap(dipResult);
        
        

        // 转换为json格式
        if (sipMap != null) {
        	syslogMap.put("geo_sip", sipMap);
        } else {
        	//syslogMap.put("geo_sip", new HashMap<String, String>());
        	syslogMap.put("geo_sip", null);
        }

        if (dipMap != null) {
        	syslogMap.put("geo_dip", dipMap);
        } else {
        	//syslogMap.put("geo_dip", new HashMap<String, String>());
        	syslogMap.put("geo_dip", null);
        }
        
        //此富化没有 webshell， webattack类型
    }
    
    
    /**
     * 转化数据类型
     *
     */
    private void enrichmentConvertDataType(String topicOutput, Map<String, Object> syslogMap) {
        switch (topicOutput) {
        case "skyeye_dns":
        	
        	Object reply_code = syslogMap.get("reply_code");
        	if(StringUtils.isNotBlank(reply_code.toString())) {
        		syslogMap.put("reply_code", Integer.parseInt(reply_code.toString()));
        	}else {
        		syslogMap.put("reply_code", 0);
        	}
        	
        	
        	//默认值
        	syslogMap.put("dns_type", 0);
        	
        	//sport ， dport
        	Object sportdns = syslogMap.get("sport");
        	syslogMap.put("sport", Integer.parseInt(sportdns.toString()));
        	Object dportdns = syslogMap.get("dport");
        	syslogMap.put("dport", Integer.parseInt(dportdns.toString()));
        	break;
        	
        case "skyeye_tcpflow":
        case "skyeye_udpflow":
            // uplink_length
            Object uplinkLengthWebLog = syslogMap.get("uplink_length");
            syslogMap.put("uplink_length", Long.parseLong(uplinkLengthWebLog.toString()));

            // downlink_length
            Object downlinkLengthWebLog = syslogMap.get("downlink_length");
            syslogMap.put("downlink_length", Long.parseLong(downlinkLengthWebLog.toString()));
            
            // uplink_pkts downlink_pkts
            Object uplink_pkts = syslogMap.get("uplink_pkts");
            syslogMap.put("uplink_pkts", Integer.parseInt(uplink_pkts.toString()));
            Object downlink_pkts = syslogMap.get("downlink_pkts");
            syslogMap.put("downlink_pkts", Integer.parseInt(downlink_pkts.toString()));
            
        default:
        	//sport ， dport
        	Object sport = syslogMap.get("sport");
        	logger.info("sport-------------is" + (String)sport);
        	syslogMap.put("sport", Integer.parseInt((String)sport));
        	Object dport = syslogMap.get("dport");
        	logger.info("sport-------------is" + (String)dport);
        	syslogMap.put("dport", Integer.parseInt((String)dport));
        	
            break;
        }
    }
    
    
    /**
     * 获取Hive分区时间
     * 
     */
    private String getPartitionTime() {
        switch (SystemMapEnrichConstants.HIVE_PARTITION_TIME_TYPE) {
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
    
	
	

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
		declarer.declare(new Fields("convertedMap", "isAlert"));
	}

}

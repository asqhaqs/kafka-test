package cn.situation.data;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import cn.situation.util.*;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;

import cn.situation.cons.SystemConstant;


public class EventTrans {
	private static final Logger LOG = LogUtil.getInstance(EventTrans.class);
	private static Map<Object,Map<String, Integer>> enrichmentAssetMap;
	private static final String redisAlertKey = SystemConstant.REDIS_KEY_PREFIX + ":" + SystemConstant.REDIS_ALERT_KEY;
	private static RedisCache<String, String> eventRedisCache = RedisUtil.getRedisCache(SystemConstant.EVENT_REDIS_CACHE);
	
	static {
		enrichmentAssetMap = getEnrichmentAsset();
	}

	/**
	 * situation—ids转换
	 * @param row
	 * @throws Exception
	 */
	public static void do_trans(String row) throws Exception {
		LOG.info(String.format("message<%s>", "mapAndEnrichOperation"), "do_trans start");
		try {
			String s_tmp = row.replace("|", "@");
			String[] fileds = s_tmp.split("@");
			do_map(fileds);
		} catch (Exception e) {
			
		}
	}
	/**
	 * 数据映射入库（redis）
	 * @param fileds
	 * @throws Exception
	 */
	private static void do_map(String[] fileds) throws Exception {
		LOG.info(String.format("message<%s>", "mapAndEnrichOperation"), "web-ids data start");

		Map<String, Object> syslogMap = new HashMap<>();

		// 导入map中
		syslogMap=fillToMap(syslogMap, fileds);
		if(syslogMap!=null) {
			// sip 和 dip 进行 ip 富化
			//GeoUtil.enrichmentIp(syslogMap);
			// 添加公共头使得该条告警通过规则引擎
			syslogMap.put("event_id", UUID.randomUUID().toString());
			syslogMap.put("found_time",
					DateUtil.timestampToDate(syslogMap.get("timestamp").toString()+"000", "yyyy-MM-dd'T'HH:mm:ss.SSSZ"));
			syslogMap.put("event_type", "005");
			syslogMap.put("event_subtype", "005100");
			syslogMap.put("industry_id", 0);
			syslogMap.put("vendor", syslogMap.get("vendor"));
			syslogMap.put("system_id", 0);
			syslogMap.put("sip", syslogMap.get("sip").toString());
			syslogMap.put("dip", syslogMap.get("dip").toString());
			syslogMap.put("organization_id", 0);
			if(StringUtils.isNotBlank(syslogMap.get("proof").toString())){
				Map<String, Object> proof = JsonUtil.jsonToMap(syslogMap.get("proof").toString());
				if(StringUtils.isNotBlank(proof.get("catalog_info").toString())) {
					syslogMap.put("attack_result", proof.get("catalog_info").toString());
				}
			}
			//单位、行业、系统、adcode孵化
			enrichmentAsset(syslogMap);
			//入redis库
			String resultJson = JsonUtil.mapToJson(syslogMap);
			eventRedisCache.rpush(redisAlertKey, resultJson);
			LOG.debug(String.format("[%s]: dicName<%s>, value<%s>", "mapAndEnrichOperation", redisAlertKey, resultJson));
		}
	}

	private static Map<Object,Map<String, Integer>> getEnrichmentAsset() {
		Map<Object,Map<String, Integer>> dataMap = new HashMap<>();
		int max_range_num = 1000;
		PgUtil pu = PgUtil.getInstance();
		PreparedStatement pre = null;
		PreparedStatement pre_sys = null;
		String sql = "SELECT distinct ips.start_ip_value,ips.end_ip_value,c.id,c.industry_id,c.canton_id FROM t_ips ips,"
				+ "t_company c WHERE ips.company_id = c.id";
		String sql_sys = "SELECT id,domain,ips FROM t_website WHERE sys_type = 1";
		try {
			pre = pu.getPreparedStatement(sql);
			
			ResultSet res = pre.executeQuery();
			while(res.next()) {
				long start_num = res.getLong(1);
				long end_num = res.getLong(2);
				if(end_num - start_num > 1000) {
					end_num = start_num + max_range_num;
				}
				for(long i = start_num; i <= end_num; i++) {
					Map<String, Integer> detailMap = new HashMap<>();
					detailMap.put("organization_id", res.getInt(3));
					detailMap.put("industry_id", res.getInt(4));
					detailMap.put("adcode", res.getInt(5));
					dataMap.put(i, detailMap);
				}
			}
			res.close();
			
			pre_sys = pu.getPreparedStatement(sql_sys);
			ResultSet res_sys = pre_sys.executeQuery();
			while(res_sys.next()) {
				String domain = res_sys.getString(2);
				String ips = res_sys.getString(3);
				Map<String, Integer> detailMap = new HashMap<>();
				detailMap.put("system_id", res_sys.getInt(1));
				dataMap.put(domain, detailMap);
				if(StringUtils.isNotBlank(ips)) {
					ips = ips.substring(1, ips.length()-1);
					if(ips.contains(",")){
						String[] all_ip = ips.split(",");
						for(String ip : all_ip) {
							int ip_num = (int)ipToLong(ip);
							dataMap.put(ip_num, detailMap);
						}
					}else {
						int ip_num = (int)ipToLong(ips);
						dataMap.put(ip_num, detailMap);
					}
				}
			}
			res_sys.close();
		} catch (Exception e) {
			LOG.error("资产重复判断失败!<%s>", e.getMessage());
		}finally {
			pu.destory();
		}
		return dataMap;
	}
	
	private static void enrichmentAsset(Map<String, Object> syslogMap) {
		// TODO Auto-generated method stub
		String ip = null;
		if(StringUtils.isNotBlank(syslogMap.get("host").toString())) {
			ip = syslogMap.get("host").toString();
		}else if(StringUtils.isNotBlank(syslogMap.get("dip").toString())) {
			ip = syslogMap.get("dip").toString();
		}
		if(StringUtils.isNotBlank(ip)) {
			Map<String, Integer> detail = null;
			if(ipCheck(ip)) {
				long ip_num = ipToLong(ip);
				detail = enrichmentAssetMap.get(ip_num);
			}else {
				detail = enrichmentAssetMap.get(ip);
			}
			if(detail != null) {
				syslogMap.put("organization_id", detail.get("organization_id"));
				syslogMap.put("industry_id", detail.get("industry_id"));
				syslogMap.put("adcode", detail.get("adcode"));
				syslogMap.put("system_id", detail.get("system_id"));
			}
		}
	}
	/**
     * 判断IP地址的合法性，这里采用了正则表达式的方法来判断
     * return true，合法
     * */
    public static boolean ipCheck(String text) {
        if (text != null && !text.isEmpty()) {
            // 定义正则表达式
            String regex = "^(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|[1-9])\\."
                      + "(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."
                      + "(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."
                      + "(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)$";
            // 判断ip地址是否与正则表达式匹配
            if (text.matches(regex)) {
                // 返回判断信息
                return true;
            } else {
                // 返回判断信息
                return false;
            }
        }
        return false;
    }
	// 将127.0.0.1形式的IP地址转换成十进制整数，这里没有进行任何错误处理  
	private static long ipToLong(String strIp) {  
        long[] ip = new long[4];  
        // 先找到IP地址字符串中.的位置  
        int position1 = strIp.indexOf(".");  
        int position2 = strIp.indexOf(".", position1 + 1);  
        int position3 = strIp.indexOf(".", position2 + 1);  
        // 将每个.之间的字符串转换成整型  
        ip[0] = Long.parseLong(strIp.substring(0, position1));  
        ip[1] = Long.parseLong(strIp.substring(position1 + 1, position2));  
        ip[2] = Long.parseLong(strIp.substring(position2 + 1, position3));  
        ip[3] = Long.parseLong(strIp.substring(position3 + 1));  
        return (ip[0] << 24) + (ip[1] << 16) + (ip[2] << 8) + ip[3];  
    }   
    
	/**
	 * 厂商数据转换
	 * @param map
	 * @param fields
	 * @return Map<String,Object
	 * @throws NullPointerException
	 * @throws ArrayIndexOutOfBoundsException
	 */
	private static Map<String, Object> fillToMap(Map<String, Object> map, String[] fields)
			throws NullPointerException, ArrayIndexOutOfBoundsException {
		Map<String, Object> map_tmp = new HashMap<String, Object>();
		if(fields.length<29) {
			return null;
		}
		//字段内的%%%转换为|
		//字段内的^^^转换\n
		try {
			for (int i = 0; i < fields.length; i++) {
				fields[i] = fields[i].replace("%%%","|");
				fields[i] = fields[i].replace("^^^","\n");
			}
		} catch (Exception e) {
		}
		//厂商字段梳理
		map_tmp.put("version", fields[0]);
		map_tmp.put("ip_type", fields[1]);
		map_tmp.put("msgtype", fields[2]);
		map_tmp.put("magic_number", fields[3]);
		map_tmp.put("timestamp", fields[4]);
		map_tmp.put("pkt_size", fields[5]);
		map_tmp.put("dev_ip", fields[6]);
		map_tmp.put("vendor", fields[7]);
		map_tmp.put("sip", fields[8]);
		map_tmp.put("dip", fields[9]);
		map_tmp.put("sport", fields[10]);
		map_tmp.put("dport", fields[11]);
		map_tmp.put("protocol_id", fields[12]);
		map_tmp.put("app_id", fields[13]);
		map_tmp.put("app_version", fields[14]);
		map_tmp.put("dev_id", fields[15]);
		map_tmp.put("sid", fields[16]);
		map_tmp.put("direct", fields[17]);
		map_tmp.put("attack_ip", fields[18]);
		map_tmp.put("attack_ipv6", fields[19]);
		map_tmp.put("victim_ip", fields[20]);
		map_tmp.put("victim_ipv6", fields[21]);
		map_tmp.put("eventcode", fields[22]);
		map_tmp.put("ruleid", fields[23]);
		map_tmp.put("vid", fields[24]);
		map_tmp.put("last_times", fields[25]);
		map_tmp.put("event_id", fields[26]);
		map_tmp.put("event_info", fields[27]);
		map_tmp.put("proof", fields[28]);
		
		// 转换
		//厂商字段
		map.put("protocol_id", fields[12]);
		map.put("app_id", fields[13]);
		map.put("app_version", fields[14]);
		map.put("dev_id", fields[15]);
		map.put("sid", fields[16]);
		map.put("direct", fields[17]);
		map.put("attack_ip", fields[18]);
		map.put("attack_ipv6", fields[19]);
		map.put("victim_ip", fields[20]);
		map.put("victim_ipv6", fields[21]);
		map.put("eventcode", fields[22]);
		map.put("ruleid", fields[23]);
		map.put("vid", fields[24]);
		map.put("last_times", fields[25]);
		map.put("event_id", fields[26]);
		map.put("event_info", fields[27]);
		map.put("proof", fields[28]);
		map.put("version", fields[0]);
		map.put("ip_type", fields[1]);
		map.put("msgtype", fields[2]);
		map.put("magic_number", fields[3]);
		map.put("timestamp", fields[4]);
		map.put("pkt_size", fields[5]);
		map.put("dev_ip", fields[6]);
		map.put("vendor", fields[7]);
		map.put("sip", fields[8]);
		map.put("dip", fields[9]);
		
		//360字段
		map.put("attack_type", "");
		map.put("bugtraq", "");
		map.put("cve", "");
		map.put("data_type", "");
		map.put("dcity", "");
		map.put("dcounty", "");
		map.put("description", "");
		map.put("dlatitude", "");
		map.put("dlongitude", "");
		map.put("dprovince", "");
		map.put("protocol", "");
		map.put("referer", "");
		map.put("agent", "");
		map.put("rsp_body_len", "");
		map.put("serial_num", "");
		map.put("rsp_content_type", "");
		map.put("parameter", "");
		map.put("method", "");
		map.put("req_body", "");
		map.put("req_header", "");
		map.put("host", "");
		map.put("cookie", "");
		map.put("uri", "");
		map.put("rsp_content_length", "");
		map.put("rsp_body", "");
		map.put("rsp_header", "");
		map.put("detail_info", "");
		map.put("confidence", "");
		map.put("vuln_harm", "");
		map.put("vuln_name", "");
		map.put("vuln_type", "");
		map.put("file", "");
		map.put("industry_name", "");
		map.put("organization_name", "");
		map.put("webrules_tag", "");
		map.put("public_date", "");
		map.put("code_language", "");
		map.put("site_app", "");
		map.put("kill_chain", "");
		map.put("device_ip", "");
		map.put("rule_name", "");
		map.put("rule_version", "");
		map.put("cnnvd_id", "");
		map.put("origin_record", "");
		map.put("origin_type", "");
		map.put("file_md5", "");
		map.put("rule_num", "");
		map.put("solution", "");
		map.put("threat_level", "");
		map.put("user", "");
		map.put("s_location", "");
		map.put("d_location", "");
		map.put("event_token", "");
		map.put("back_up", "");
		
		//共有字段
		map.put("dport", map_tmp.get("dport"));
		map.put("sport", map_tmp.get("sport"));
		map.put("sip", map_tmp.get("sip"));
		map.put("dip", map_tmp.get("dip"));
		map.put("rule_num", map_tmp.get("ruleid"));
		map.put("description", map_tmp.get("event_info"));
		map.put("rule_name", map_tmp.get("event_info"));
		map.put("sess_id", map_tmp.get("sid"));
		map.put("vendor_id", map_tmp.get("vendor_id"));
		return map;
	}
	public static void main(String[] args) throws Exception {
		List<String> s_list = new ArrayList<String>();
//		s_list.add(
//				"0x01|0x01|0x020B|0x01|1541560149|100|127.0.0.1|360|1.2.3.4|4.3.2.1|0|0| tcp|||12345678qwertyuiasdfghjkzxcvbnml123456|01|127.0.0.1||127.0.0.1||1||||5|ddos|{“http_keypayload”: “ab%%%cdef^^^111111”}\r\n");
//		s_list.add(
//				"0x01|0x01|0x020B|0x01|1541560149|100|127.0.0.1|360|1.2.3.4|4.3.2.1|0|0| tcp|||12345678qwertyuiasdfghjkzxcvbnml123456|01|127.0.0.1||127.0.0.1||1||||5|ddos|{“http_keypayload”: “ab%%%cdef^^^111111”}\r\n");
//		s_list.add(
//				"0x01|0x01|0x020B|0x01|1541560149|100|127.0.0.1|360|1.2.3.4|4.3.2.1|0|0| tcp|||12345678qwertyuiasdfghjkzxcvbnml123456|01|127.0.0.1||127.0.0.1||1||||5|ddos|{“http_keypayload”: “ab%%%cdef^^^111111”}\r\n");
//		s_list.add(
//				"0x01|0x01|0x020B|0x01|1541560149|100|127.0.0.1|360|1.2.3.4|4.3.2.1|0|0| tcp|||12345678qwertyuiasdfghjkzxcvbnml123456|01|127.0.0.1||127.0.0.1||1||||5|ddos|{“http_keypayload”: “ab%%%cdef^^^111111”}\r\n");
//		s_list.add(
//				"0x01|0x01|0x020B|0x01|1541560149|100|127.0.0.1|360|1.2.3.4|4.3.2.1|0|0| tcp|||12345678qwertyuiasdfghjkzxcvbnml123456|01|127.0.0.1||127.0.0.1||1||||5|ddos|{“http_keypayload”: “ab%%%cdef^^^111111”}\r\n");
//		s_list.add(
//				"0x01|0x01|0x020B|0x01|1541560149|100|127.0.0.1|360|1.2.3.4|4.3.2.1|0|0| tcp|||12345678qwertyuiasdfghjkzxcvbnml123456|01|127.0.0.1||127.0.0.1||1||||5|ddos|{“http_keypayload”: “ab%%%cdef^^^111111”}\r\n");
//		s_list.add(
//				"0x01|0x01|0x020B|0x01|1541560149|100|127.0.0.1|360|1.2.3.4|4.3.2.1|0|0| tcp|||12345678qwertyuiasdfghjkzxcvbnml123456|01|127.0.0.1||127.0.0.1||1||||5|ddos|{“http_keypayload”: “ab%%%cdef^^^111111”}\r\n");
//		s_list.add(
//				"0x01|0x01|0x020B|0x01|1541560149|100|127.0.0.1|360|1.2.3.4|4.3.2.1|0|0| tcp|||12345678qwertyuiasdfghjkzxcvbnml123456|01|127.0.0.1||127.0.0.1||1||||5|ddos|{“http_keypayload”: “ab%%%cdef^^^111111”}\r\n");
//		s_list.add(
//				"0x01|0x01|0x020B|0x01|1541560149|100|127.0.0.1|360|1.2.3.4|4.3.2.1|0|0| tcp|||12345678qwertyuiasdfghjkzxcvbnml123456|01|127.0.0.1||127.0.0.1||1||||5|ddos|{“http_keypayload”: “ab%%%cdef^^^111111”}\r\n");
//		do_trans(s_list);
		Map<Object,Map<String, Integer>> map = getEnrichmentAsset();
		System.out.println(map.size());
		do_trans("0x^^^01|0x01|0x0100|305441741|1544067360|1187||360|6.6.6.3|2.0.0.2|58102|80|6|4071||e179dd7caf1ce974e9a8985869b21d5fc1b30855|0x5C0899200001BE03||2.0.0.2||6.6.6.3|||92||1|||{\"log_proof_cp\": {\"download_file_info\": {\"filemd5\": \"\", \"filesize\": 0, \"filename\": \"\"}, \"domain\": \"\", \"attack_tool\": \"\", \"tcp_keypayload\": [], \"account_info\": {\"username\": \"\", \"password\": \"\"}, \"udp_keypayload\": [], \"anom_traffic_statistics\": {\"durations\": 0, \"sessions\": 0}, \"mail_keypayload\": [], \"src_port\": 58102, \"ip_list\": [], \"application\": \"WGET\", \"http_keypayload\": [], \"file_info\": {\"filemd5\": \"\", \"filesize\": 0, \"filename\": \"\"}, \"event_abstract\": \"\", \"src_ip\": \"6.6.6.3\", \"source\": \"2.0.0.2\\/026A1A95FC065249C7A974EB4E0520D1.6C14E3ED\", \"device_info\": {\"dev_type\": \"\", \"dev_name\": \"\", \"serial\": \"e179dd7caf1ce974e9a8985869b21d5fc1b30855\"}, \"dark_ip\": \"\", \"scan_tool\": \"\", \"icmp_keypayload\": [], \"third_party\": \"\", \"cve_id\": \"\", \"catalog_info\": \"\", \"cnvd_id\": \"\", \"telnet_keypayload\": [], \"dns_keypayload\": [], \"dark_domain\": \"\", \"database_info\": {\"db_type\": \"\", \"db_name\": \"\"}, \"vulnerability_info\": \"\", \"sample_abstract\": {\"name\": \"026A1A95FC065249C7A974EB4E0520D1\", \"family\": \"Virus\\/Win32.Sality.q\", \"md5\": \"026A1A95FC065249C7A974EB4E0520D1\"}, \"dst_port\": 80, \"ftp_keypayload\": [], \"action\": \"Virus\", \"dst_ip\": \"2.0.0.2\", \"attack_signature\": \"\"}}\r\n");
	}
}

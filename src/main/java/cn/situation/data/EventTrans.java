package cn.situation.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;

import cn.situation.cons.SystemConstant;
import cn.situation.util.DateUtil;
import cn.situation.util.DicUtil;
import cn.situation.util.Geoip;
import cn.situation.util.JsonUtil;
import cn.situation.util.LogUtil;



public class EventTrans {
	private static final Logger LOG = LogUtil.getInstance(EventTrans.class);
	
	private static final String redisAlertKey = SystemConstant.REDIS_KEY_PREFIX + ":" + SystemConstant.REDIS_ALERT_KEY;

	/**
	 * situation—ids转换
	 * @param e_list
	 * @throws Exception
	 */
	public static void do_trans(List<String> e_list) throws Exception {
		LOG.info(String.format("message<%s>", "mapAndEnrichOperation"), "do_trans start");
		for (String row : e_list) {
			String s_tmp = row.replace("|", "%");
			String[] fileds = s_tmp.split("%");
			do_map(fileds);
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
//			enrichmentIp(syslogMap, true);
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
			
			//入redis库
			String resultJson = JsonUtil.mapToJson(syslogMap);
			System.out.println(resultJson);
			DicUtil.rpush(redisAlertKey, resultJson, SystemConstant.KIND_ASSERT);
			LOG.info(String.format("[%s]: dicName<%s>, value<%s>", "mapAndEnrichOperation", redisAlertKey, resultJson));
		}
	}
	/**
	 * 富化ip
	 * @param syslogMap
	 * @param isAleart
	 * @throws Exception
	 */
	private static void enrichmentIp(Map<String, Object> syslogMap, Boolean isAleart) throws Exception {
		String sipStr;
		String dipStr;

		sipStr = (null == syslogMap.get("sip")) ? null : (String) syslogMap.get("sip");
		dipStr = (null == syslogMap.get("dip")) ? null : (String) syslogMap.get("dip");

		Geoip.Result sipResult = Geoip.getInstance().query(sipStr);
		Geoip.Result dipResult = Geoip.getInstance().query(dipStr);

		Map<String, String> sipMap = Geoip.convertResultToMap(sipResult);
		Map<String, String> dipMap = Geoip.convertResultToMap(dipResult);

		// 转换为json格式
		if (sipMap != null) {
			syslogMap.put("geo_sip", sipMap);
		} else {
			syslogMap.put("geo_sip", null);
		}
		if (dipMap != null) {
			syslogMap.put("geo_dip", dipMap);
		} else {
			syslogMap.put("geo_dip", null);
		}
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
		if(fields.length<28) {
			return null;
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
		
		s_list.add("0x01|0x01|0x0100|305441741|1544067360|1187||360|6.6.6.3|2.0.0.2|58102|80|6|4071||e179dd7caf1ce974e9a8985869b21d5fc1b30855|0x5C0899200001BE03||2.0.0.2||6.6.6.3|||92||1|||{\"log_proof_cp\": {\"download_file_info\": {\"filemd5\": \"\", \"filesize\": 0, \"filename\": \"\"}, \"domain\": \"\", \"attack_tool\": \"\", \"tcp_keypayload\": [], \"account_info\": {\"username\": \"\", \"password\": \"\"}, \"udp_keypayload\": [], \"anom_traffic_statistics\": {\"durations\": 0, \"sessions\": 0}, \"mail_keypayload\": [], \"src_port\": 58102, \"ip_list\": [], \"application\": \"WGET\", \"http_keypayload\": [], \"file_info\": {\"filemd5\": \"\", \"filesize\": 0, \"filename\": \"\"}, \"event_abstract\": \"\", \"src_ip\": \"6.6.6.3\", \"source\": \"2.0.0.2\\/026A1A95FC065249C7A974EB4E0520D1.6C14E3ED\", \"device_info\": {\"dev_type\": \"\", \"dev_name\": \"\", \"serial\": \"e179dd7caf1ce974e9a8985869b21d5fc1b30855\"}, \"dark_ip\": \"\", \"scan_tool\": \"\", \"icmp_keypayload\": [], \"third_party\": \"\", \"cve_id\": \"\", \"catalog_info\": \"\", \"cnvd_id\": \"\", \"telnet_keypayload\": [], \"dns_keypayload\": [], \"dark_domain\": \"\", \"database_info\": {\"db_type\": \"\", \"db_name\": \"\"}, \"vulnerability_info\": \"\", \"sample_abstract\": {\"name\": \"026A1A95FC065249C7A974EB4E0520D1\", \"family\": \"Virus\\/Win32.Sality.q\", \"md5\": \"026A1A95FC065249C7A974EB4E0520D1\"}, \"dst_port\": 80, \"ftp_keypayload\": [], \"action\": \"Virus\", \"dst_ip\": \"2.0.0.2\", \"attack_signature\": \"\"}}\r\n");
		do_trans(s_list);
	}
}

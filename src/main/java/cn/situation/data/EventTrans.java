package cn.situation.data;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import cn.situation.util.*;
import org.slf4j.Logger;

import cn.situation.cons.SystemConstant;


public class EventTrans {
	private static final Logger LOG = LogUtil.getInstance(EventTrans.class);
	
	private static final String redisAlertKey = SystemConstant.REDIS_KEY_PREFIX + ":" + SystemConstant.REDIS_ALERT_KEY;

	/**
	 * situation—ids转换
	 * @param line
	 * @throws Exception
	 */
	public static void do_trans(String line) throws Exception {
		LOG.debug(String.format("message<%s>", "mapAndEnrichOperation"), "do_trans start");
		String s_tmp = line.replace("|", "@");
		String[] fileds = s_tmp.split("@");
		do_map(fileds);
	}
	/**
	 * 数据映射入库（redis）
	 * @param fileds
	 * @throws Exception
	 */
	private static void do_map(String[] fileds) throws Exception {
		LOG.debug(String.format("message<%s>", "mapAndEnrichOperation"), "web-ids data start");

		Map<String, Object> syslogMap = new HashMap<>();

		// 导入map中
		syslogMap=fillToMap(syslogMap, fileds);
		if (syslogMap!=null) {
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
			
			//入redis库
			String resultJson = JsonUtil.mapToJson(syslogMap);
			DicUtil.rpush(redisAlertKey, resultJson, SystemConstant.KIND_EVENT);
			LOG.debug(String.format("[%s]: dicName<%s>, value<%s>", "mapAndEnrichOperation", redisAlertKey, resultJson));
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
	private static Map<String, Object> fillToMap(Map<String, Object> map, String[] fields) throws Exception {
		Map<String, Object> map_tmp = new HashMap<String, Object>();
		if (fields.length < 29) {
			return null;
		}
		//字段内的%%%转换为|
		//字段内的^^^转换\n
		for (int i = 0; i < fields.length; i++) {
			fields[i] = fields[i].replace("%%%","|");
			fields[i] = fields[i].replace("^^^","\n");
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
		map.put("attack_result", "");
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
		map.put("sess_id", map_tmp.get("sid"));
		map.put("vendor_id", map_tmp.get("vendor_id"));
		return map;
	}
	public static void main(String[] args) throws Exception {

	}
}

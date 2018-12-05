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

/**
 * 告警数据映射转换
 */
public class EventTrans {

	private static final Logger LOG = LogUtil.getInstance(EventTrans.class);
	private static final String redisAlertKey = SystemConstant.REDIS_KEY_PREFIX + ":" + SystemConstant.REDIS_ALERT_KEY;

	public static Object do_trans(List<String> e_list) throws Exception {
		for (String row : e_list) {
			String[] fileds = row.split("//|");
			do_map(fileds);
		}
		return null;
	}
	
	public static Object do_map(String[] fileds) throws Exception {
//		LOG.info(String.format("[%s]: isAlert<%s>, message<%s>", "mapAndEnrichOperation"));
		
		Map<String, Object> syslogMap = new HashMap<>();

		// 导入map中
		// fillToMap(syslogMap, alertFields, fieldList);
		
		// // 加入 360 字段，全部置空
		// for (String qhField : qhAlertFields) {
		// syslogMap.put(qhField, null);
		// }
		 // sip 和 dip 进行 ip 富化
		 enrichmentIp(syslogMap, true);

		// 添加公共头使得该条告警通过规则引擎
		syslogMap.put("event_id", UUID.randomUUID().toString());
		syslogMap.put("found_time",
				DateUtil.timestampToDate(syslogMap.get("timestamp").toString(), "yyyy-MM-dd'T'HH:mm:ss.SSSZ"));
		syslogMap.put("event_type", "005");
		syslogMap.put("event_subtype", "005100");
		syslogMap.put("industry_id", 0);
		syslogMap.put("vendor", syslogMap.get("vendor"));
		syslogMap.put("system_id", 0);
		syslogMap.put("sip", syslogMap.get("sip").toString());
		syslogMap.put("dip", syslogMap.get("dip").toString());
		syslogMap.put("organization_id", 0);
		String resultJson = JsonUtil.mapToJson(syslogMap);
		DicUtil.rpush(redisAlertKey, resultJson);
		LOG.info(String.format("[%s]: dicName<%s>, value<%s>", "mapAndEnrichOperation", redisAlertKey, resultJson));
		return null;
	}

	/**
	 * 富化ip(sip、dip; Webids：victim、attacker)
	 * 
	 * @param syslogMap
	 * @param isAleart
	 * @throws Exception
	 */
	private static void enrichmentIp(Map<String, Object> syslogMap, Boolean isAleart) throws Exception {
		String sipStr;
		String dipStr;
		// if (!isAleart) {
		// // （1）sip、dip
		sipStr = (null == syslogMap.get("sip")) ? null : (String) syslogMap.get("sip");
		dipStr = (null == syslogMap.get("dip")) ? null : (String) syslogMap.get("dip");
		// } else {
		// sipStr = (null == syslogMap.get("sip")) ? null :
		// (String)syslogMap.get("src_ip");
		// dipStr = (null == syslogMap.get("dip")) ? null :
		// (String)syslogMap.get("dst_ip");
		// }

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
		// 此富化没有 webshell， webattack类型
	}

	public static void main(String[] args) throws Exception {
		List<String> s_list = new ArrayList<String>();
		s_list.add(
				"0x01|0x01|0x020B|0x01|1541560149|100|127.0.0.1|360|1.2.3.4|4.3.2.1|0|0| tcp|||12345678qwertyuiasdfghjkzxcvbnml123456|01|127.0.0.1||127.0.0.1||1||||5|ddos|{“http_keypayload”: “ab%%%cdef^^^111111”}\r\n");
		s_list.add(
				"0x01|0x01|0x020B|0x01|1541560149|100|127.0.0.1|360|1.2.3.4|4.3.2.1|0|0| tcp|||12345678qwertyuiasdfghjkzxcvbnml123456|01|127.0.0.1||127.0.0.1||1||||5|ddos|{“http_keypayload”: “ab%%%cdef^^^111111”}\r\n");
		s_list.add(
				"0x01|0x01|0x020B|0x01|1541560149|100|127.0.0.1|360|1.2.3.4|4.3.2.1|0|0| tcp|||12345678qwertyuiasdfghjkzxcvbnml123456|01|127.0.0.1||127.0.0.1||1||||5|ddos|{“http_keypayload”: “ab%%%cdef^^^111111”}\r\n");
		s_list.add(
				"0x01|0x01|0x020B|0x01|1541560149|100|127.0.0.1|360|1.2.3.4|4.3.2.1|0|0| tcp|||12345678qwertyuiasdfghjkzxcvbnml123456|01|127.0.0.1||127.0.0.1||1||||5|ddos|{“http_keypayload”: “ab%%%cdef^^^111111”}\r\n");
		s_list.add(
				"0x01|0x01|0x020B|0x01|1541560149|100|127.0.0.1|360|1.2.3.4|4.3.2.1|0|0| tcp|||12345678qwertyuiasdfghjkzxcvbnml123456|01|127.0.0.1||127.0.0.1||1||||5|ddos|{“http_keypayload”: “ab%%%cdef^^^111111”}\r\n");
		s_list.add(
				"0x01|0x01|0x020B|0x01|1541560149|100|127.0.0.1|360|1.2.3.4|4.3.2.1|0|0| tcp|||12345678qwertyuiasdfghjkzxcvbnml123456|01|127.0.0.1||127.0.0.1||1||||5|ddos|{“http_keypayload”: “ab%%%cdef^^^111111”}\r\n");
		s_list.add(
				"0x01|0x01|0x020B|0x01|1541560149|100|127.0.0.1|360|1.2.3.4|4.3.2.1|0|0| tcp|||12345678qwertyuiasdfghjkzxcvbnml123456|01|127.0.0.1||127.0.0.1||1||||5|ddos|{“http_keypayload”: “ab%%%cdef^^^111111”}\r\n");
		s_list.add(
				"0x01|0x01|0x020B|0x01|1541560149|100|127.0.0.1|360|1.2.3.4|4.3.2.1|0|0| tcp|||12345678qwertyuiasdfghjkzxcvbnml123456|01|127.0.0.1||127.0.0.1||1||||5|ddos|{“http_keypayload”: “ab%%%cdef^^^111111”}\r\n");
		s_list.add(
				"0x01|0x01|0x020B|0x01|1541560149|100|127.0.0.1|360|1.2.3.4|4.3.2.1|0|0| tcp|||12345678qwertyuiasdfghjkzxcvbnml123456|01|127.0.0.1||127.0.0.1||1||||5|ddos|{“http_keypayload”: “ab%%%cdef^^^111111”}\r\n");
		do_trans(s_list);
	}

	// /**
	// * 将日志中的数据导入至map中
	// * @param map
	// * @param fields
	// * @param logValues
	// * @throws NullPointerException
	// * @throws ArrayIndexOutOfBoundsException
	// */
	// private void fillToMap(Map<String,Object> map, String[] fields, List<String>
	// logValues)
	// throws NullPointerException,ArrayIndexOutOfBoundsException{
	// //对syslog 内容数组而不是字段数组进行遍历，防止因为数据过长截断而导致的数组越界问题
	// for (int i = 0; i < logValues.size(); i++) {
	// map.put(fields[i].trim(), logValues.get(i).trim());
	// }
	// }
}

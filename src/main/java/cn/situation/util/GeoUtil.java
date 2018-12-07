package cn.situation.util;

import java.util.Map;

/**
 * @author lenzhao
 * @date 2018/12/7 20:08
 * @description TODO
 */
public class GeoUtil {

    /**
     * 富化ip
     * @param syslogMap
     * @throws Exception
     */
    public static void enrichmentIp(Map<String, Object> syslogMap) throws Exception {
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
}

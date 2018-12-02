package cn.situation.util;

import org.apache.commons.lang3.StringUtils;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {

    /**
     * 当前时间格式化“yyyy-MM-dd HH:mm:ss.SSS”字符串
     * @return
     */
    public static String formatNowTime() {
        Date currentTime = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String dateString = formatter.format(currentTime);
        return dateString;
    }

    /**
     * 当前时间格式化“yyyy-MM”字符串--月
     * @return
     */
    public static String formatNowMonthTime() {
        Date currentTime = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM");
        String dateString = formatter.format(currentTime);
        return dateString;
    }

    /**
     * 当前时间格式化“yyyy-MM-dd”字符串--天
     * @return
     */
    public static String formatNowDayTime() {
        Date currentTime = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        String dateString = formatter.format(currentTime);
        return dateString;
    }

    /**
     * 当前时间格式化“yyyy-MM-dd HH”字符串--小时
     * @return
     */
    public static String formatNowHourTime() {
        Date currentTime = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH");
        String dateString = formatter.format(currentTime);
        return dateString;
    }

    /**
     * 将时间戳转化为 “yyyy-MM-dd HH:mm:ss” 字符串
     */
    public static String timestampToDate(String timestamp, String format){
        if(StringUtils.isEmpty(timestamp)) {
            return "";
        }
        if(StringUtils.isEmpty(format)){
            format = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
        }
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
        return simpleDateFormat.format(new Date(Long.valueOf(timestamp)));
    }
}

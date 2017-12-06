
package soc.storm.situation.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 
 * @author wangbin03
 *
 */
public class DateTimeUtils {

    /**
     * 当前时间格式化“yyyy-MM-dd HH:mm:ss.SSS”字符串
     * 
     * @return
     */
    public static String formatNowTime() {
        Date currentTime = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String dateString = formatter.format(currentTime);
        return dateString;
    }

    /**
     * 当前时间格式化“yyyy-MM-dd HH”字符串--小时
     * 
     * @return
     */
    public static String formatNowHourTime() {
        Date currentTime = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH");
        String dateString = formatter.format(currentTime);
        return dateString;
    }
}
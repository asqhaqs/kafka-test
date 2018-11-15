package soc.storm.situation.test.utils;

import soc.storm.situation.utils.DateTimeUtils;

public class DateTimeTest {
    public static void main(String[] args) {
        System.out.println(DateTimeUtils.timestampToDate("1528872351938", "yyyy-MM-dd HH:mm:ss"));
    }
}

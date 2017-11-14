
package soc.storm.situation.test.compress;

import java.io.IOException;

import soc.storm.situation.compress.Lz4Compress;

/**
 * 
 * @author wangbin03
 *
 */
public class Lz4Test {

    // private static String sourceStr = "我是中国人我是中国人我是中国人我是中国人我是中国人我是中国人我是中国人我是中国人我是中国人我是中国人我是中国人我是中国人我是中国人我是中国人我是中国人"
    // + "我是中国人我是中国人我是中国人我是中国人我是中国人我是中国人我是中国人我是中国人我是中国人我是中国人"
    // + "我是中国人我是中国人我是中国人我是中国人我是中国人我是中国人我是中国人我是中国人我是中国人我是中国人"
    // + "我是中国人我是中国人我是中国人我是中国人我是中国人我是中国人我是中国人我是中国人";

    private static String sourceStr = "为了提供更好的用户体验，同时让更多人享用云计算的技术红利，态势感知（基础版）现做如下功能调整："
            + "将全面开放DDoS事件，页面篡改，肉鸡行为，蠕虫病毒，暴力破解成功，后门shell，主机攻击等7种安全告警的告警详情和分析报告不再提供流量趋势，"
            + "访问分析，威胁分析，情报等功能，将迁移至专业版中以上调整将于8月10日~8月21日分批进行。";

    public static void main(String[] args) throws IOException {
        System.out.println("------------sourceStr:" + sourceStr);
        // int inputLength = sourceStr.getBytes("UTF-8").length;
        int inputLength = sourceStr.getBytes().length;
        System.out.println("------------inputLength:" + inputLength);

        //
        // byte[] compressByteArray = compress001(sourceStr);
        byte[] compressByteArray = Lz4Compress.compress(sourceStr.getBytes("UTF-8"));
        System.out.println("------------compressByteArray.length:" + compressByteArray.length);

        //
        String deCommpressStr = new String(Lz4Compress.deCommpress(compressByteArray));
        System.out.println("------------deCommpressStr:" + deCommpressStr);
    }

}

// <dependency>
// <groupId>org.anarres.lzo</groupId>
// <artifactId>lzo-core</artifactId>
// <version>1.0.0</version>
// </dependency>
// <dependency>
// <span style="white-space:pre"> </span><groupId>org.anarres.lzo</groupId>
// <span style="white-space:pre"> </span><artifactId>lzo-hadoop</artifactId>
// <span style="white-space:pre"> </span><version>1.0.0</version>
// </dependency>
// <dependency>
// <groupId>org.apache.hadoop</groupId>
// <artifactId>hadoop-core</artifactId>
// <version>1.0.3</version>
// </dependency>

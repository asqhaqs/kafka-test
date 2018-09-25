
package soc.storm.situation.encrypt;

import org.apache.storm.shade.org.apache.commons.codec.digest.DigestUtils;

/**
 * 
 * @author wangbin03
 *
 */
public class Md5Util {

    /**
     * demo：
     * "host": "client.api.ttpod.com",
     * "host_md5": "74175e132f3db71c4c166a835063ce5b",
     * https://md5jiami.51240.com/
     * 32位小写
     * 
     * 就是有些情况host是这样client.api.ttpod.com:8080
     * 后面有冒号加端口
     * 这个原来是去掉冒号和后面的端口然后再计算md5
     * 
     * 
     * 201801241800
     * 路娟：http://client.api.ttpod.com:65533 这种不处理，没见过这样的，只处理client.api.ttpod.com:65533，ftp的不处理，没见过
     * 
     * 路娟:
     * 只做了一次判断
     * 路娟:
     * http://client.api.ttpod.com:65533 这种不处理
     * 路娟:
     * 没见过这样的
     * 路娟:
     * 只处理client.api.ttpod.com:65533
     * 路娟:
     * ftp的不处理，没见过
     * 路娟:
     * client.api.ttpod.com:65533
     * http://client.api.ttpod.com
     * 这两种情况
     * 路娟:
     * 就是：只出现一次
     * 路娟:
     * 出现两次的就不管了
     * 路娟:
     * ftp://的也不处理
     * 路娟:
     * 这些都没见过
     * 路娟:
     * 如果你有这种特殊情况，可以反馈一下，加进去
     * 
     * @param sourceStr
     * @return
     */
    public static String enrichmentHostMd5Hex(String sourceStr) {
        String[] sourceStrSplitArray = sourceStr.split(":");

        String validMd5Str = sourceStr;
        if (2 == sourceStrSplitArray.length) {
            if (sourceStrSplitArray[0].equals("http")) {
                int httpIndex = sourceStr.indexOf("http://");
                if (-1 != httpIndex) {
                    validMd5Str = sourceStr.substring("http://".length());
                }
            } else if (isNumeric(sourceStrSplitArray[1])) {
                validMd5Str = sourceStrSplitArray[0];
            }
        }

        return DigestUtils.md5Hex(validMd5Str).toLowerCase();
    }

    /**
     * 判断字符串是否为数字
     * 
     * @param str
     * @return
     */
    public static boolean isNumeric(String str) {
        for (int i = str.length(); --i >= 0;) {
            if (!Character.isDigit(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * 
     * @param args
     */
    public static void main(String[] args) {
        System.out.println(enrichmentHostMd5Hex("client.api.ttpod.com"));
        System.out.println(enrichmentHostMd5Hex("client.api.ttpod.com:8080"));
        System.out.println(enrichmentHostMd5Hex("http://client.api.ttpod.com"));
        System.out.println(enrichmentHostMd5Hex("http://client.api.ttpod.com:8080"));
        System.out.println(enrichmentHostMd5Hex("ftp://client.api.ttpod.com"));
        System.out.println(enrichmentHostMd5Hex("ftp://client.api.ttpod.com:65533"));
    }
}


package soc.storm.situation.coder;

/**
 * WebLogGatherMsg
 * 
 * {
 * count; //4个字节，二进制数据，日志总数。
 * {
 * len_0; //4个字节，二进制数据，第1个日志长度。
 * data_0; //日志数据，长度与由len0指定。
 * }.......
 * {
 * len_n; //4个字节，二进制数据，第n个日志长度。
 * data_n; //日志数据，长度与由len_n指定。
 * }
 * }
 * 
 * @author wangbin03
 *
 */
public class WebFlowLogGatherMsg {

    /**
     * 
     * @author wangbin03
     *
     */
    static class WebFlowLogEntity {
        private int len;// 4个字节，二进制数据，第1个日志长度。
        private byte[] data;// 日志数据，长度与由len0指定。

        public int getLen() {
            return len;
        }

        public void setLen(int len) {
            this.len = len;
        }

        public byte[] getData() {
            return data;
        }

        public void setData(byte[] data) {
            this.data = data;
        }
    }

    private int count;// 4个字节，二进制数据，日志总数。
    private WebFlowLogEntity[] webFlowLogEntities;// 日志数据, 长度与由count指定。

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public WebFlowLogEntity[] getWebFlowLogEntities() {
        return webFlowLogEntities;
    }

    public void setWebFlowLogEntities(WebFlowLogEntity[] webFlowLogEntities) {
        this.webFlowLogEntities = webFlowLogEntities;
    }

}

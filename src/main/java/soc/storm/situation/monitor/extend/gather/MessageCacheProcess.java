
package soc.storm.situation.monitor.extend.gather;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 描述: MessageQueue 处理类
 * 
 * @author wangbin03
 */
public class MessageCacheProcess<T> {
    private static Logger logger = LoggerFactory.getLogger(MessageCacheProcess.class);

    private BlockingQueue<T> messageCache = new LinkedBlockingQueue<T>(1000000);

    /**
     * 获取message
     * 
     * @return
     */
    public T getMessage() {
        try {
            return messageCache.take();
        } catch (InterruptedException e) {
            throw new RuntimeException("[getMessage] error: InterruptedException");
        }
    }

    /**
     * 将message存入队列
     * 
     * @param rawDataList
     * @return
     */
    public void addMessage(T message) {
        try {
            messageCache.put(message);
        } catch (InterruptedException e) {
            throw new RuntimeException("[addMessage] error: InterruptedException");
        }
    }

    /**
     * 获取原始告警信息存入队列大小
     * 
     * @return
     */
    public int getCurrentMessageCount() {
        int messageCacheSize = messageCache.size();
        logger.info("[getCurrentMessageCount] messageCacheSize: {}", messageCacheSize);

        return messageCacheSize;
    }

}

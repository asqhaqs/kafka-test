
package cn.situation;

import cn.situation.cons.SystemConstant;
import cn.situation.jobs.ProducerWorker;
import cn.situation.util.LogUtil;
import org.slf4j.Logger;

import java.util.Vector;

public class KafkaProducerTest {

    private static final Logger LOG = LogUtil.getInstance(KafkaProducerTest.class);

    public static void main(String[] args) {
        LOG.info("producer starting.");
        String[] topics = SystemConstant.TOPIC_LIST.split(",");
        if (topics.length > 0) {
            Vector<Thread> threadVector = new Vector<>();
            for (String topic : topics) {
                Thread thread = new Thread(new ProducerWorker(topic, Integer.parseInt(args[0])));
                threadVector.add(thread);
                thread.start();
            }
            for (Thread thread : threadVector) {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            LOG.info("producer finished.");
        }
    }
}

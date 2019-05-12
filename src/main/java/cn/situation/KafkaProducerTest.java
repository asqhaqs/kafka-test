
package cn.situation;

import cn.situation.cons.SystemConstant;
import cn.situation.jobs.ProducerWorker;

public class KafkaProducerTest {

    public static void main(String[] args) {
        String[] topics = SystemConstant.TOPIC_LIST.split(",");
        if (topics.length > 0) {
            for (String topic : topics) {
                Thread thread = new Thread(new ProducerWorker(topic, Integer.parseInt(args[0])));
                thread.start();
            }
            try {
                Thread.currentThread().join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

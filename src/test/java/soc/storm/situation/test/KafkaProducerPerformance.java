
package soc.storm.situation.test;

import soc.storm.situation.test.KafkaProducerTask.KafkaProducerExecutorServiceTask;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class KafkaProducerPerformance {

    public static void main(String[] args) {
        KafkaProducerExecutorServiceTask kafkaProducerExecutorServiceTask = new KafkaProducerExecutorServiceTask();
        ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
        service.scheduleAtFixedRate(kafkaProducerExecutorServiceTask, 1, 100, TimeUnit.SECONDS);

        // try {
        // Thread.sleep(500000);
        // } catch (InterruptedException e) {
        // e.printStackTrace();
        // }
        // service.shutdown();
    }

}

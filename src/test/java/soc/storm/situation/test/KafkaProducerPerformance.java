
package soc.storm.situation.test;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import soc.storm.situation.test.KafkaProducerTask.KafkaProducerExecutorServiceTask;

public class KafkaProducerPerformance {

    public static void main(String[] args) {
        KafkaProducerExecutorServiceTask kafkaProducerExecutorServiceTask = new KafkaProducerExecutorServiceTask();
        ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
        // service.scheduleAtFixedRate(kafkaProducerExecutorServiceTask, 1, 100, TimeUnit.SECONDS);// dns
        service.scheduleAtFixedRate(kafkaProducerExecutorServiceTask, 1, 200, TimeUnit.SECONDS);// tcpflow

        // try {
        // Thread.sleep(500000);
        // } catch (InterruptedException e) {
        // e.printStackTrace();
        // }
        // service.shutdown();
    }

}

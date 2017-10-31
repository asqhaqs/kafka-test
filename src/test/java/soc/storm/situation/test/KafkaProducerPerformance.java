
package soc.storm.situation.test;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import soc.storm.situation.test.KafkaProducerTask.KafkaProducerExecutorServiceTask;

public class KafkaProducerPerformance {

    public static void main(String[] args) {
        // Runnable runnable = new Runnable() {
        // public void run() {
        // System.out.println("---------------------");
        // // try {
        // // Thread.sleep(30000L);
        // // } catch (InterruptedException e) {
        // // e.printStackTrace();
        // // }
        // }
        // };

        KafkaProducerExecutorServiceTask kafkaProducerExecutorService = new KafkaProducerExecutorServiceTask();
        ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
        service.scheduleAtFixedRate(kafkaProducerExecutorService, 1, 100, TimeUnit.SECONDS);

        // try {
        // Thread.sleep(500000);
        // } catch (InterruptedException e) {
        // e.printStackTrace();
        // }
        // service.shutdown();
    }

}

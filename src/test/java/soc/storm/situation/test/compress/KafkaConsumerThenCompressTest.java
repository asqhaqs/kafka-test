
package soc.storm.situation.test.compress;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import soc.storm.situation.coder.WebFlowLogGatherMsgBinCoder;
import soc.storm.situation.coder.WebFlowLogGatherMsgCoder;
import soc.storm.situation.compress.Bzip2Compress;
import soc.storm.situation.compress.GzipCompress;
import soc.storm.situation.compress.Lz4Compress;
import soc.storm.situation.compress.LzoCompress;
import soc.storm.situation.compress.SnappyCompress;
import soc.storm.situation.compress.XzCompress;

public class KafkaConsumerThenCompressTest extends Thread {
    private final KafkaConsumer<String, Object> consumer;
    private final String topic;

    public KafkaConsumerThenCompressTest(String topic) {
        this.consumer = new KafkaConsumer<String, Object>(createConsumerConfig());
        this.topic = topic;
    }

    private static Properties createConsumerConfig() {
        Properties properties = new Properties();
        // properties.put("bootstrap.servers", "172.24.2.155:9092,172.24.2.156:9092,172.24.2.157:9092");
        properties.put("bootstrap.servers", "bg02.situation.360es.net:9092,bg04.situation.360es.net:9092,bg05.situation.360es.net:9092");
        // properties.put("group.id", SystemConstants.TOPOLOGY_NAME);
        // properties.put("group.id", "zhongsanmu1119009");
        properties.put("group.id", "zhongsanmu20180315002");
        properties.put("enable.auto.commit", "false");
        properties.put("auto.commit.interval.ms", "1000");
        // properties.put("auto.offset.reset", "earliest");// String must be one of: latest, earliest, none
        properties.put("auto.offset.reset", "earliest");// String must be one of: latest, earliest, none
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RangeAssignor");

        return properties;
    }

    /**
     * 
     * getByteArray used by KafkaProducerTask 原始天眼流量日志数据
     * 
     * @param tupleCount
     * @return
     */
    public byte[] getByteArrayInit(int tupleTotalCount) {
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            //
            ConsumerRecords<String, Object> records = consumer.poll(100);
            consumer.commitSync();
            // System.out.println("--------records.count():" + records.count());

            for (ConsumerRecord<String, Object> consumerRecord : records) {
                // （2）sensor protocol--add zhongsanmu 20171130
                byte[] skyeyeWebFlowLogByteArray = (byte[]) consumerRecord.value();
                return skyeyeWebFlowLogByteArray;
            }

        }
    }

    /**
     * 
     * getByteArray used by KafkaProducerTask
     * 
     * @param tupleCount
     * @return
     */
    public byte[] getByteArray(int tupleTotalCount) {
        consumer.subscribe(Arrays.asList(topic));
        long sumCount = 0;
        boolean isFinish = false;
        List<Object> pbBytesTcpFlowList = new ArrayList<Object>();
        while (true) {
            //
            ConsumerRecords<String, Object> records = consumer.poll(100);
            consumer.commitSync();
            // System.out.println("--------records.count():" + records.count());

            for (ConsumerRecord<String, Object> consumerRecord : records) {
                // System.out.println(String.format("offset=%d,key=%s,value=%s",
                // consumerRecord.offset(),
                // consumerRecord.key(),
                // consumerRecord.value()));

                // //
                // byte[] consumerRecordValue = getByteArray(consumerRecord.value());
                // System.out.println("--------------consumerRecordValue:" + consumerRecordValue.length);
                //

                // （2）sensor protocol--add zhongsanmu 20171130
                byte[] skyeyeWebFlowLogByteArray = (byte[]) consumerRecord.value();

                WebFlowLogGatherMsgCoder webFlowLogGatherMsgCoder = new WebFlowLogGatherMsgBinCoder();
                List<Object> pbBytesWebFlowLogList = webFlowLogGatherMsgCoder.fromWire(skyeyeWebFlowLogByteArray);
                for (Object object : pbBytesWebFlowLogList) {
                    sumCount++;
                    pbBytesTcpFlowList.add(consumerRecord.value());

                    if (tupleTotalCount < sumCount) {
                        isFinish = true;
                        break;
                    }
                }
                if (true == isFinish) {
                    break;
                }

                // sumCount++;
                // pbBytesTcpFlowList.add(consumerRecord.value());
                //
                // if (tupleTotalCount < sumCount) {
                // isFinish = true;
                // break;
                // }
            }

            if (true == isFinish) {
                break;
            }
        }

        // source
        byte[] sourceData = getByteArray(pbBytesTcpFlowList);
        return sourceData;
    }

    @Override
    public void run() {
        int tupleTotalCount = 10000;

        // source
        // byte[] sourceData = getByteArray(pbBytesTcpFlowList);
        byte[] sourceData = getByteArray(tupleTotalCount);

        // lzo
        byte[] lzoCompressData = LzoCompress.compress(sourceData);
        // snappy
        byte[] snappyCompressData = SnappyCompress.compress(sourceData);
        // lz4
        byte[] lz4CompressData = Lz4Compress.compress(sourceData);
        // gzip
        byte[] gzipCompressData = GzipCompress.compress(sourceData);
        // xz--lzma
        byte[] xzCompressData = XzCompress.compress(sourceData);
        // bzip2
        byte[] bzip2CompressData = Bzip2Compress.compress(sourceData);

        System.out.println("sumCount:" + tupleTotalCount
                + ", sourceLength:" + sourceData.length
                + ", lzoCompressLength:" + lzoCompressData.length
                + ", snappyCompressLength:" + snappyCompressData.length
                + ", lz4CompressLength:" + lz4CompressData.length
                + ", gzipCompressLength:" + gzipCompressData.length
                + ", lzmaCompressLength:" + xzCompressData.length
                + ", bzip2CompressLength:" + bzip2CompressData.length
                );
    }

    /**
     * java Serializable
     * 
     * @param o
     * @return
     * @throws IOException
     */
    public static byte[] getByteArray(final Object o) {
        try {
            byte[] resultByteArray = {};
            if (o == null) {
                return resultByteArray;
            }

            ByteArrayOutputStream buf = new ByteArrayOutputStream(4096);
            ObjectOutputStream out = new ObjectOutputStream(buf);
            out.writeObject(o);
            out.flush();
            buf.close();
            return buf.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        // ty_tcpflow ty_dns
        KafkaConsumerThenCompressTest consumerTest = new KafkaConsumerThenCompressTest("ty_tcpflow");
        consumerTest.start();
    }

}

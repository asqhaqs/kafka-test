package cn.situation.service;

import cn.situation.util.LogUtil;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class OffsetLoggingCallbackImpl implements OffsetCommitCallback, ConsumerRebalanceListener {
    private static final Logger LOG = LogUtil.getInstance(OffsetLoggingCallbackImpl.class);
    private Map<TopicPartition, OffsetAndMetadata> partitionOffsetMap = new ConcurrentHashMap<>();

    @Override
    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
        if (exception == null) {
            offsets.forEach((topicPartition, offsetAndMetadata) -> {
                partitionOffsetMap.computeIfPresent(topicPartition, (k, v) -> offsetAndMetadata);
                LOG.debug(String.format("[%s]: threadName<%s>, partition<%s>, offset<%s>", "onComplete",
                        Thread.currentThread().getName(), topicPartition.partition(), offsetAndMetadata.offset()));
            });
        } else {
            offsets.forEach((topicPartition, offsetAndMetadata) ->
                    LOG.error(String.format("[%s]: threadName<%s>, partition<%s>, offset<%s>, message<%s>", "onComplete",
                            Thread.currentThread().getName(), topicPartition.partition(), offsetAndMetadata.offset(),
                            exception.getMessage()), exception)
            );
        }
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        LOG.info("Partitions revoked: {}", partitions);
        for (TopicPartition currentPartition : partitions) {
            partitionOffsetMap.remove(currentPartition);
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        LOG.info("Partitions assigned: {}", partitions);
        for (TopicPartition currentPartition : partitions) {
            partitionOffsetMap.put(currentPartition, new OffsetAndMetadata(0L, "Initial default offset"));
        }
    }

    public Map<TopicPartition, OffsetAndMetadata> getPartitionOffsetMap() {
        return partitionOffsetMap;
    }
}

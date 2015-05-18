package storm.kafka.internal;

import storm.kafka.Partition;

/**
 * unique identifier for a message in kafka
 */
public class KafkaMessageId {

    private Partition partition;
    private long offset;

    public KafkaMessageId(Partition partition, long offset) {
        this.partition = partition;
        this.offset = offset;
    }

    public Partition getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    @Override
    public String toString() {
        return "KafkaMessageId{" +
                "partition=" + partition +
                ", offset=" + offset +
                '}';
    }
}
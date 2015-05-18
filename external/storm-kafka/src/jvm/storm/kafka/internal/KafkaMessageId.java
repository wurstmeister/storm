package storm.kafka.internal;

import storm.kafka.Partition;

import java.util.Objects;

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
    public int hashCode() {
        return Objects.hash(partition, offset);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final KafkaMessageId other = (KafkaMessageId) obj;
        return Objects.equals(this.partition, other.partition)
                && Objects.equals(this.offset, other.offset);
    }

    @Override
    public String toString() {
        return "KafkaMessageId{" +
                "partition=" + partition +
                ", offset=" + offset +
                '}';
    }
}
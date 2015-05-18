package storm.kafka.internal;


import com.google.common.base.Preconditions;

import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Maintains the set of offsets emitted for a partitions and
 * keeps track of which offset has been acked last
 */
public class MessageHandler {

    private Long lastCommitted = 0L;

    private SortedSet<Long> offsets = new TreeSet<Long>();

    public MessageHandler(Long lastCommitted) {
        Preconditions.checkNotNull(lastCommitted, "last commited offset can't be null");
        this.lastCommitted = lastCommitted;
    }

    public void add(KafkaMessageId message) {
        offsets.add(message.getOffset());
    }

    public void ack(KafkaMessageId message) {
        offsets.remove(message.getOffset());
        lastCommitted = Math.max(message.getOffset(), lastCommitted);
    }

    public void fail(KafkaMessageId message) {
        offsets.remove(message.getOffset());
    }

    public Long commitOffset() {
        return lastCommitted;
    }
}

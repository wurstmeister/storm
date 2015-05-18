package storm.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * Partition messages in kafka by key so that we can
 * write predictable tests
 */
public class KeyBasedPartitioner implements Partitioner {

    public KeyBasedPartitioner(VerifiableProperties props) {
    }

    public int partition(Object key, int partitions) {
        return key.toString().hashCode();
    }
}

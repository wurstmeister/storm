package storm.kafka.internal;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import kafka.javaapi.consumer.SimpleConsumer;
import storm.kafka.*;
import storm.kafka.trident.IBrokerReader;

import java.io.Closeable;
import java.util.*;


/**
 * Kafka consumer for Storm.
 *
 * The aim is to be as API compatible as possible with the new Kafka 0.8.3 consumer to ease migration
 * (@see http://kafka.apache.org/083/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html)
 *
 */
public class StormKafkaConsumer implements Closeable{

    private final DynamicPartitionConnections connections;
    private final IBrokerReader brokerReader;
    private final KafkaConfig spoutConfig;
    private final Map stormConf;
    private final Map<Partition, PartitionManager> managers;
    private final Map<Partition, Long> offsets;

    public StormKafkaConsumer(final KafkaConfig spoutConfig, Map conf, TopologyContext context) {
        this.stormConf = conf;
        this.spoutConfig = spoutConfig;
        brokerReader = KafkaUtils.makeBrokerReader(conf, spoutConfig);
        connections = new DynamicPartitionConnections(spoutConfig, brokerReader);
        managers = new HashMap<Partition, PartitionManager>();

//        context.registerMetric("kafkaOffset", new IMetric() {
//            KafkaUtils.KafkaOffsetMetric _kafkaOffsetMetric = new KafkaUtils.KafkaOffsetMetric(spoutConfig.topic, connections);
//
//            @Override
//            public Object getValueAndReset() {
//                List<PartitionManager> pms = new LinkedList<PartitionManager>(managers.values());
//                Set<Partition> latestPartitions = new HashSet();
//                for (PartitionManager pm : pms) {
//                    latestPartitions.add(pm.getPartition());
//                }
//                _kafkaOffsetMetric.refreshPartitions(latestPartitions);
//                for (PartitionManager pm : pms) {
//                    _kafkaOffsetMetric.setLatestEmittedOffset(pm.getPartition(), pm.lastCompletedOffset());
//                }
//                return _kafkaOffsetMetric.getValueAndReset();
//            }
//        }, spoutConfig.metricsTimeBucketSizeInSecs);
//
//        context.registerMetric("kafkaPartition", new IMetric() {
//            @Override
//            public Object getValueAndReset() {
//                List<PartitionManager> pms = new LinkedList<PartitionManager>(managers.values());
//                Map concatMetricsDataMaps = new HashMap();
//                for (PartitionManager pm : pms) {
//                    concatMetricsDataMaps.putAll(pm.getMetricsDataMap());
//                }
//                return concatMetricsDataMaps;
//            }
//        }, spoutConfig.metricsTimeBucketSizeInSecs);
        offsets = new HashMap<Partition, Long>();
    }

    /**
     * subscribe to the specified partition
     *
     * @param partition
     */
    public void subscribe(Partition partition) {
        if (!managers.containsKey(partition)) {
            PartitionManager man = new PartitionManager(connections, stormConf, spoutConfig, partition);
            managers.put(partition, man);
        }
    }

    /**
     * Fetches data for the partitions specified.
     * The offset used for fetching the data is governed by whether or not seek(Partition) is used.
     * If seek(Partition) is used, it will use the specified offsets on startup to consume data from that offset sequentially on every poll.
     * If not, it will use the offset configured in the KafkaConfig#startOffsetTime
     *
     * This method attempts to return one record per partition to ensure we can honour MAX_SPOUT_PENDING as much as possible.
     * If the configured scheme results in multiple tuples those will be returned in a single poll
     *
     * @return a list of records from each of the managed partitions
     */
    public List<StormKafkaRecord> poll() {
        Collection<PartitionManager> partitionManagers = managers.values();
        List<StormKafkaRecord> result = new LinkedList<StormKafkaRecord>();
        CapturingSpoutCollector collector = new CapturingSpoutCollector();
        SpoutOutputCollector spoutOutputCollector = new SpoutOutputCollector(collector);
        for (PartitionManager partitionManager : partitionManagers) {
            partitionManager.next(spoutOutputCollector);
            for (StormKafkaRecord record : collector.getRecords()) {
                result.add(record);
                offsets.put(partitionManager.getPartition(), record.getMessageId().getOffset());
            }
            collector.reset();
        }
        return result;
    }

    /**
     * get a list of all partitions that this consumer is subscribed to
     * @return
     */
    public Set<Partition> subscriptions() {
        return managers.keySet();
    }

    public List<Partition> partitionsFor() {
        return brokerReader.getCurrentBrokers().getOrderedPartitions();
    }

    public void seek(Partition partition, long offset) {
        if ( offset == kafka.api.OffsetRequest.LatestTime() || offset == kafka.api.OffsetRequest.EarliestTime()) {
            seekToSpecial(partition, offset);
        } else {
            managers.get(partition).setOffset(offset);
            offsets.put(partition, offset);
        }
    }

    /**
     * Seek to the first offset for the given partition
     * @param partition
     */
    public void seekToBeginning(Partition partition) {
        seekToSpecial(partition, kafka.api.OffsetRequest.EarliestTime());
    }

    private void seekToSpecial(Partition partition, long startOffsetTime) {
        SimpleConsumer consumer = managers.get(partition).getConsumer();
        long newOffset = KafkaUtils.getOffset(consumer, spoutConfig.topic, partition.partition, startOffsetTime);
        seek(partition, newOffset);
    }

    /**
     * Seek to the last offset for the given partition
     * @param partition
     */
    public void seekToEnd(Partition partition) {
        seekToSpecial(partition, kafka.api.OffsetRequest.LatestTime());
    }


    /**
     * required to support the PartitionManager API
     * @param id
     */
    public void fail(KafkaMessageId id) {
        managers.get(new Partition(null, id.getPartition().partition)).fail(id.getOffset());
    }

    /**
     * required to support the PartitionManager API
     * @param id
     */
    public void finish(KafkaMessageId id) {
        managers.get(new Partition(null, id.getPartition().partition)).ack(id.getOffset());
    }

    public void close() {
        for (PartitionManager partitionManager : managers.values()) {
            partitionManager.close();
        }
    }

    /**
     * Returns the offset of the next record that will be fetched or returned if it is already buffered (if a record with that offset exists).
     * @param partition
     * @return the offset
     */
    public long position(Partition partition) {
        return offsets.get(partition) + 1;
    }


    /**
     * returns the last emitted offset from the underlying buffer
     * @param partition
     * @return
     */
    public long bufferedPosition(Partition partition) {
        return managers.get(partition).getOffset();
    }
}

package storm.kafka.internal;

import backtype.storm.Config;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.TopologyContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import storm.kafka.*;
import storm.kafka.trident.GlobalPartitionInformation;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;


public class StormKafkaConsumerTest {


    @Mock
    private TopologyContext context;
    private KafkaTestBroker broker;
    private StaticHosts brokerHosts;
    private SpoutConfig config;
    private StormKafkaConsumer consumer;
    private Partition partition = new Partition(null, 0);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        setupConfigWithNPartitions(1);
        consumer = new StormKafkaConsumer(config, new Config(), context);
    }

    @After
    public void shutdown() {
        broker.shutdown();
    }

    private void setupConfigWithNPartitions(int numPartitions) {
        broker = new KafkaTestBroker(numPartitions);
        GlobalPartitionInformation globalPartitionInformation = new GlobalPartitionInformation();
        for (int partition = 0 ; partition < numPartitions; partition ++) {
            globalPartitionInformation.addPartition(partition, Broker.fromString(broker.getBrokerConnectionString()));
        }
        brokerHosts = new StaticHosts(globalPartitionInformation);
        config = new SpoutConfig(brokerHosts, "testTopic", "/testing", "test-id");
        config.scheme = new SchemeAsMultiScheme(new StringScheme());
    }

    @Test
    public void testSubscribe() throws Exception {
        consumer.subscribe(partition);
        Set<Partition> subscriptions = consumer.subscriptions();
        assertEquals(1, subscriptions.size());
        assertEquals(partition, subscriptions.iterator().next());
    }

    @Test
    public void testPoll() throws Exception {
        String value = "value";
        TestUtils.createTopicAndSendMessage(broker, config.topic, "0", value);
        consumer.subscribe(partition);
        consumer.seekToBeginning(partition);
        List<StormKafkaRecord> records = consumer.poll();
        assertEquals(1, records.size());
        assertEquals(value, records.get(0).getTuple().get(0));
    }

    @Test
    public void testPartitionsFor() throws Exception {
        List<Partition> partitions = consumer.partitionsFor();
        assertEquals(1, partitions.size());
        assertEquals(partition, partitions.get(0));
    }

    @Test
    public void testSeek() throws Exception {
        for (int i = 0 ; i < 10; i++ ) {
            TestUtils.createTopicAndSendMessage(broker, config.topic, "0", "" + i);
        }
        consumer.subscribe(partition);
        int offset = 3;
        consumer.seek(partition, offset);
        List<StormKafkaRecord> records = consumer.poll();
        assertEquals(1, records.size());
        assertEquals(offset + "", records.get(0).getTuple().get(0));
    }

    @Test
    public void testSeekToEnd() throws Exception {
        int numMessages = 10;
        for (int i = 0 ; i < numMessages; i++ ) {
            TestUtils.createTopicAndSendMessage(broker, config.topic, "0", "" + i);
        }
        consumer.subscribe(partition);
        consumer.seekToEnd(partition);
        int expectedValue = numMessages + 1;
        TestUtils.createTopicAndSendMessage(broker, config.topic, "0", "" + expectedValue);
        List<StormKafkaRecord> records = consumer.poll();
        assertEquals(1, records.size());
        assertEquals(expectedValue + "", records.get(0).getTuple().get(0));
    }

    @Test
    public void testPosition() throws Exception {
        int numMessages = 10;
        for (int i = 0 ; i < numMessages; i++ ) {
            TestUtils.createTopicAndSendMessage(broker, config.topic, "0", "" + i);
        }
        consumer.subscribe(partition);
        consumer.seekToEnd(partition);
        long position = consumer.position(partition);
        assertEquals(11, position);
    }

    @Test
    public void testPositionAfterPoll() throws Exception {
        int numMessages = 10;
        for (int i = 0 ; i < numMessages; i++ ) {
            TestUtils.createTopicAndSendMessage(broker, config.topic, "0", "" + i);
        }
        consumer.subscribe(partition);
        consumer.seekToBeginning(partition);

        List<StormKafkaRecord> records = consumer.poll();
        assertEquals(1, records.size());

        long position = consumer.position(partition);
        assertEquals(1, position);

    }


    @Test
    public void testBufferedPosition() throws Exception {
        int numMessages = 10;
        for (int i = 0 ; i < numMessages; i++ ) {
            TestUtils.createTopicAndSendMessage(broker, config.topic, "0", "" + i);
        }
        consumer.subscribe(partition);
        consumer.seekToBeginning(partition);

        List<StormKafkaRecord> records = consumer.poll();
        assertEquals(1, records.size());

        long position = consumer.bufferedPosition(partition);
        assertEquals(10, position);

    }
}
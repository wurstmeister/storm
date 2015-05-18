package storm.kafka;

import backtype.storm.Config;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import storm.kafka.internal.CapturingSpoutCollector;
import storm.kafka.internal.MessageHandler;
import storm.kafka.internal.StateHandler;
import storm.kafka.internal.StormKafkaRecord;
import storm.kafka.trident.GlobalPartitionInformation;

import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class KafkaSpoutTest {

    private KafkaTestBroker broker;
    private SpoutConfig config;
    private BrokerHosts brokerHosts;
    @Mock
    private TopologyContext context;
    @Mock
    private StateHandler stateHandler;


    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        when(context.getThisComponentId()).thenReturn("test");
        when(context.getComponentTasks("test")).thenReturn(Lists.newArrayList(1));
        when(context.getThisTaskIndex()).thenReturn(0);
    }

    @After
    public void shutdown() {
        broker.shutdown();
    }

    @Test
    public void sendSingleMessageAndAck() throws Exception {
        setupConfigWithNPartitions(1);

        String value = "value";
        TestUtils.createTopicAndSendMessage(broker, config.topic, "0", value);

        CapturingSpoutCollector capture = new CapturingSpoutCollector();

        KafkaSpout spout = buildKafkaSpout(capture);

        spout.nextTuple();

        List<StormKafkaRecord> records = capture.getRecords();
        assertEquals(records.size(), 1);
        StormKafkaRecord record = records.get(0);
        assertEquals(value, record.getTuple().get(0));

        verifyNoMoreTuples(capture, spout);
        verifyAck(spout, 0, record);
    }

    private KafkaSpout buildKafkaSpout(CapturingSpoutCollector capture) {
        KafkaSpout spout = new KafkaSpout(config, stateHandler);
        Config stormConfig = new Config();
        spout.open(stormConfig, context, new SpoutOutputCollector(capture));
        return spout;
    }

    @Test
    public void sendSingleMessageWithMultiplePartitions() throws Exception {
        int numPartitions = 2;
        setupConfigWithNPartitions(numPartitions);

        String value = "value";
        TestUtils.createTopicAndSendMessage(broker, config.topic, "0", value + "0");
        TestUtils.createTopicAndSendMessage(broker, config.topic, "1", value + "1");

        CapturingSpoutCollector capture = new CapturingSpoutCollector();
        KafkaSpout spout = buildKafkaSpout(capture);
        spout.nextTuple();

        List<StormKafkaRecord> records = capture.getRecords();

        assertEquals(numPartitions, records.size());

        for (StormKafkaRecord record : records) {
            assertEquals(value + record.getMessageId().getPartition().partition, record.getTuple().get(0));
        }

        verifyNoMoreTuples(capture, spout);
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
        when(stateHandler.lastOffset(any(Partition.class), anyString())).thenReturn(config.startOffsetTime);
    }

    private void verifyAck(KafkaSpout spout, int partition, StormKafkaRecord lastRecord) {
        MessageHandler messageHandler = spout.getMessageHandlers().get(partition);
        assertEquals(Long.valueOf(config.startOffsetTime), messageHandler.commitOffset());
        spout.ack(lastRecord.getMessageId());
        assertEquals(Long.valueOf(0), messageHandler.commitOffset());
    }

    @Test
    public void commitAckedMessageIds() throws Exception {
        setupConfigWithNPartitions(1);

        String value = "value";
        for ( int i = 0; i < 10; i++) {
            TestUtils.createTopicAndSendMessage(broker, config.topic, "0", value + i);
        }

        CapturingSpoutCollector capture = new CapturingSpoutCollector();

        KafkaSpout spout = buildKafkaSpout(capture);

        long lastAcked = 0;
        for ( int i = 0 ; i < 4 ; i++) {
            spout.nextTuple();
            List<StormKafkaRecord> records = capture.getRecords();
            assertEquals(records.size(), 1);
            StormKafkaRecord record = records.get(0);
            assertEquals(record.getTuple().get(0), value + i);
            spout.ack(record.getMessageId());
            lastAcked = record.getMessageId().getOffset();
            capture.reset();
        }

        Thread.sleep(config.stateUpdateIntervalMs + 100);

        spout.nextTuple();

        ArgumentCaptor<Long> argument = ArgumentCaptor.forClass(Long.class);
        verify(stateHandler).persist(any(Partition.class), anyString(), argument.capture(), anyString(), anyString() );
        assertEquals(Long.valueOf(lastAcked), argument.getValue());

    }


    @Test
    public void testPartitionAssignment() throws Exception {
        when(context.getComponentTasks("test")).thenReturn(Lists.newArrayList(1, 2));
        when(context.getThisTaskIndex()).thenReturn(0);
        int numPartitions = 2;
        setupConfigWithNPartitions(numPartitions);


        String value = "value";
        TestUtils.createTopicAndSendMessage(broker, config.topic, "0", value + "0");
        TestUtils.createTopicAndSendMessage(broker, config.topic, "1", value + "1");

        CapturingSpoutCollector capture = new CapturingSpoutCollector();
        KafkaSpout spout = buildKafkaSpout(capture);
        spout.nextTuple();

        List<StormKafkaRecord> records = capture.getRecords();

        assertEquals(1, records.size());
        StormKafkaRecord record = records.get(0);
        assertEquals(value + record.getMessageId().getPartition().partition, record.getTuple().get(0));


        verifyNoMoreTuples(capture, spout);
    }

    private void verifyNoMoreTuples(CapturingSpoutCollector capture, KafkaSpout spout) {
        capture.reset();
        spout.nextTuple();
        assertTrue(capture.getRecords().isEmpty());
    }


    //test failed tuples
}
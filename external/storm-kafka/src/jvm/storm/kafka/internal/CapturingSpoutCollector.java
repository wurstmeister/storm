package storm.kafka.internal;

import backtype.storm.spout.ISpoutOutputCollector;

import java.util.ArrayList;
import java.util.List;

/**
 * spout collector that can be queried for emitted records
 */
public class CapturingSpoutCollector implements ISpoutOutputCollector {

    private List<StormKafkaRecord> records = new ArrayList<StormKafkaRecord>();

    @Override
    public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
        records.add(new StormKafkaRecord(tuple, (KafkaMessageId)messageId));
        return null;
    }

    @Override
    public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
        records.add(new StormKafkaRecord(tuple, (KafkaMessageId)messageId));
    }

    @Override
    public void reportError(Throwable error) {

    }

    /**
     * get all records captured since creation or the last reset
     * @return
     */
    public List<StormKafkaRecord> getRecords() {
        return records;
    }

    /**
     * clear all captured records
     */
    public void reset() {
        records.clear();
    }

}

package storm.kafka;


import storm.kafka.internal.KafkaMessageId;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TestRetryManager implements FailedMsgRetryManager{

    private Set<KafkaMessageId> failedMassages = new HashSet<KafkaMessageId>();

    @Override
    public void open(SpoutConfig kafkaConfig) {

    }

    @Override
    public void failed(KafkaMessageId messageId) {
        failedMassages.add(messageId);
    }

    @Override
    public void acked(KafkaMessageId offset) {
        failedMassages.remove(offset);
    }

    @Override
    public void retryStarted(KafkaMessageId offset) {

    }

    @Override
    public KafkaMessageId nextFailedMessageToRetry() {
        if ( !failedMassages.isEmpty() ) {
            return failedMassages.iterator().next();
        }
        return null;
    }

    @Override
    public boolean shouldRetryMsg(KafkaMessageId offset) {
        return failedMassages.contains(offset);
    }
}

package storm.kafka;


import storm.kafka.internal.KafkaMessageId;

public class DefaultFailedMessageRetryHandler implements FailedMsgRetryManager {

    @Override
    public void open(SpoutConfig kafkaConfig) {

    }

    @Override
    public void failed(KafkaMessageId messageId) {

    }

    @Override
    public void acked(KafkaMessageId offset) {

    }

    @Override
    public void retryStarted(KafkaMessageId offset) {

    }

    @Override
    public KafkaMessageId nextFailedMessageToRetry() {
        return null;
    }

    @Override
    public boolean shouldRetryMsg(KafkaMessageId offset) {
        return false;
    }
}

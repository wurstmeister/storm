package storm.kafka.internal;


import java.util.List;

public class StormKafkaRecord {

    private KafkaMessageId messageId;

    private List<Object> tuple;


    public StormKafkaRecord(List<Object> tuple, KafkaMessageId messageId) {
        this.tuple = tuple;
        this.messageId = messageId;
    }


    public KafkaMessageId getMessageId() {
        return messageId;
    }

    public List<Object> getTuple() {
        return tuple;
    }
}

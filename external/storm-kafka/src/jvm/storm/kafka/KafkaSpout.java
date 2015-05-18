/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.kafka;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import kafka.api.OffsetRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.internal.KafkaMessageId;
import storm.kafka.internal.MessageHandler;
import storm.kafka.internal.StateHandler;
import storm.kafka.internal.StormKafkaConsumer;
import storm.kafka.internal.StormKafkaRecord;

import java.util.*;

public class KafkaSpout extends BaseRichSpout {

    public static final Logger LOG = LoggerFactory.getLogger(KafkaSpout.class);
    private final SpoutConfig spoutConfig;
    private StormKafkaConsumer consumer;

    private String uuid = UUID.randomUUID().toString();
    private SpoutOutputCollector collector;

    long lastUpdateMs = System.currentTimeMillis();

    private int totalTasks;
    private int taskIndex;
    private List<Partition> topicPartitions;
    private StateHandler stateHandler;
    private Map stormConf;
    private Map<Integer, MessageHandler> messageHandlers;


    public KafkaSpout(SpoutConfig spoutConf) {
        this(spoutConf, new StateHandler());
    }

    public KafkaSpout(SpoutConfig spoutConfig, StateHandler stateHandler) {
        this.spoutConfig = spoutConfig;
        this.stateHandler = stateHandler;
    }


    @Override
    public void open(Map conf, final TopologyContext context, final SpoutOutputCollector collector) {
        this.collector = collector;
        this.messageHandlers = new HashMap<Integer, MessageHandler>();
        this.stormConf = conf;
        this.consumer = new StormKafkaConsumer(spoutConfig, stormConf, context);
        if ( stateHandler == null ) {
            stateHandler = new StateHandler();
        }
        this.stateHandler.prepare(conf, spoutConfig);
        // using TransactionalState like this is a hack
        this.totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
        this.taskIndex = context.getThisTaskIndex();
        this.topicPartitions = myPartitions(consumer.partitionsFor().size());
        for (Partition topicPartition : topicPartitions) {
            consumer.subscribe(topicPartition);
            Long offset = stateHandler.lastOffset(topicPartition, uuid);
            if (offset != null) {
                consumer.seek(topicPartition, offset);
            } else {
                if (spoutConfig.startOffsetTime == OffsetRequest.EarliestTime()) {
                    consumer.seekToBeginning(topicPartition);
                } else {
                    consumer.seekToEnd(topicPartition);
                }
            }
            messageHandlers.put(topicPartition.partition, new MessageHandler(offset));
        }
    }


    /**
     * get the offset for the partition specified
     * @param paritition
     * @return
     */
    private long getOffsetFor(Partition paritition) {
        Long persistedOffset = stateHandler.lastOffset(paritition, uuid);
        Long currentPosition = consumer.position(paritition);
        if (consumer.position(paritition) - persistedOffset > spoutConfig.maxOffsetBehind || currentPosition <= 0) {
            LOG.info("Last commit offset from zookeeper: " + currentPosition);
            Long lastCommittedOffset = currentPosition;
            currentPosition = persistedOffset;
            LOG.info("Commit offset " + lastCommittedOffset + " is more than " +
                    spoutConfig.maxOffsetBehind + " behind latest offset " + persistedOffset + ", resetting to startOffsetTime=" + spoutConfig.startOffsetTime);
        }
        return currentPosition;
    }

    @Override
    public void close() {
        stateHandler.close();
    }

    @Override
    public void nextTuple() {
        List<StormKafkaRecord> records = consumer.poll();
        for (StormKafkaRecord record : records) {
            KafkaMessageId messageId = record.getMessageId();
            collector.emit(record.getTuple(), messageId);
            messageHandlers.get(messageId.getPartition().partition).add(messageId);
        }
        long now = System.currentTimeMillis();
        if ((now - lastUpdateMs) > spoutConfig.stateUpdateIntervalMs) {
            commit();
        }
    }

    @Override
    public void ack(Object msgId) {
        KafkaMessageId id = (KafkaMessageId) msgId;
        messageHandlers.get(id.getPartition().partition).ack(id);
        consumer.finish(id);
    }

    @Override
    public void fail(Object msgId) {
        KafkaMessageId id = (KafkaMessageId) msgId;
        messageHandlers.get(id.getPartition().partition).fail(id);
        consumer.fail(id);
    }

    @Override
    public void deactivate() {
        commit();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(spoutConfig.scheme.getOutputFields());
    }

    private void commit() {
        lastUpdateMs = System.currentTimeMillis();
        for (Partition topicPartition : topicPartitions) {
            Long commitOffset = messageHandlers.get(topicPartition.partition).commitOffset();
            if (commitOffset != null) {
                stateHandler.persist(topicPartition, spoutConfig.topic, commitOffset, uuid, (String) stormConf.get(Config.TOPOLOGY_NAME));
            }
        }
    }

    private List<Partition> myPartitions(int numPartitions) {
        Preconditions.checkArgument(taskIndex < totalTasks, "task index must be less that total tasks");
        if (numPartitions < totalTasks) {
            LOG.warn("there are more tasks than partitions (tasks: " + totalTasks + "; partitions: " + numPartitions + "), some tasks will be idle");
        }
        List<Partition> taskPartitions = new ArrayList<Partition>();
        for (int i = taskIndex; i < numPartitions; i += totalTasks) {
            Partition taskPartition = new Partition(i);
            taskPartitions.add(taskPartition);
        }
        return taskPartitions;
    }

    @VisibleForTesting
    protected Map<Integer, MessageHandler> getMessageHandlers() {
        return messageHandlers;
    }
}

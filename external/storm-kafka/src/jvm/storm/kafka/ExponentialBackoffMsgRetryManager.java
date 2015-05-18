/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.kafka;

import storm.kafka.internal.KafkaMessageId;

import java.util.*;

public class ExponentialBackoffMsgRetryManager implements FailedMsgRetryManager {

    private long retryInitialDelayMs;
    private double retryDelayMultiplier;
    private long retryDelayMaxMs;

    private Queue<MessageRetryRecord> waiting = new PriorityQueue<MessageRetryRecord>(11, new RetryTimeComparator());
    private Map<KafkaMessageId,MessageRetryRecord> records = new HashMap<KafkaMessageId,MessageRetryRecord>();

    @Override
    public void open(SpoutConfig spoutConfig) {
        this.retryInitialDelayMs = spoutConfig.retryInitialDelayMs;
        this.retryDelayMultiplier = spoutConfig.retryDelayMultiplier;
        this.retryDelayMaxMs = spoutConfig.retryDelayMaxMs;
    }

    @Override
    public void failed(KafkaMessageId messageId) {
        MessageRetryRecord oldRecord = this.records.get(messageId);
        MessageRetryRecord newRecord = oldRecord == null ?
                                       new MessageRetryRecord(messageId) :
                                       oldRecord.createNextRetryRecord();
        this.records.put(messageId, newRecord);
        this.waiting.add(newRecord);
    }

    @Override
    public void acked(KafkaMessageId offset) {
        MessageRetryRecord record = this.records.remove(offset);
        if (record != null) {
            this.waiting.remove(record);
        }
    }

    @Override
    public void retryStarted(KafkaMessageId messageId) {
        MessageRetryRecord record = this.records.get(messageId);
        if (record == null || !this.waiting.contains(record)) {
            throw new IllegalStateException("cannot retry a message that has not failed");
        } else {
            this.waiting.remove(record);
        }
    }

    @Override
    public KafkaMessageId nextFailedMessageToRetry() {
        if (this.waiting.size() > 0) {
            MessageRetryRecord first = this.waiting.peek();
            if (System.currentTimeMillis() >= first.retryTimeUTC) {
                if (this.records.containsKey(first.messageId)) {
                    return first.messageId;
                } else {
                    // defensive programming - should be impossible
                    this.waiting.remove(first);
                    return nextFailedMessageToRetry();
                }
            }
        }
        return null;
    }

    @Override
    public boolean shouldRetryMsg(KafkaMessageId messageId) {
        MessageRetryRecord record = this.records.get(messageId);
        return record != null &&
                this.waiting.contains(record) &&
                System.currentTimeMillis() >= record.retryTimeUTC;
    }

    /**
     * A MessageRetryRecord holds the data of how many times a message has
     * failed and been retried, and when the last failure occurred.  It can
     * determine whether it is ready to be retried by employing an exponential
     * back-off calculation using config values stored in SpoutConfig:
     * <ul>
     *  <li>retryInitialDelayMs - time to delay before the first retry</li>
     *  <li>retryDelayMultiplier - multiplier by which to increase the delay for each subsequent retry</li>
     *  <li>retryDelayMaxMs - maximum retry delay (once this delay time is reached, subsequent retries will
     *                        delay for this amount of time every time)
     *  </li>
     * </ul>
     */
    class MessageRetryRecord {
        private final KafkaMessageId messageId;
        private final int retryNum;
        private final long retryTimeUTC;

        public MessageRetryRecord(KafkaMessageId messageId) {
            this(messageId, 1);
        }

        private MessageRetryRecord(KafkaMessageId messageId, int retryNum) {
            this.messageId = messageId;
            this.retryNum = retryNum;
            this.retryTimeUTC = System.currentTimeMillis() + calculateRetryDelay();
        }

        /**
         * Create a MessageRetryRecord for the next retry that should occur after this one.
         * @return MessageRetryRecord with the next retry time, or null to indicate that another
         *         retry should not be performed.  The latter case can happen if we are about to
         *         run into the backtype.storm.Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS in the Storm
         *         configuration.
         */
        public MessageRetryRecord createNextRetryRecord() {
            return new MessageRetryRecord(this.messageId, this.retryNum + 1);
        }

        private long calculateRetryDelay() {
            double delayMultiplier = Math.pow(retryDelayMultiplier, this.retryNum - 1);
            double delay = retryInitialDelayMs * delayMultiplier;
            Long maxLong = Long.MAX_VALUE;
            long delayThisRetryMs = delay >= maxLong.doubleValue()
                                    ?  maxLong
                                    : (long) delay;
            return Math.min(delayThisRetryMs, retryDelayMaxMs);
        }

        @Override
        public int hashCode() {
            return Objects.hash(messageId);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            final MessageRetryRecord other = (MessageRetryRecord) obj;
            return Objects.equals(this.messageId, other.messageId);
        }
    }

    class RetryTimeComparator implements Comparator<MessageRetryRecord> {

        @Override
        public int compare(MessageRetryRecord record1, MessageRetryRecord record2) {
            return Long.valueOf(record1.retryTimeUTC).compareTo(Long.valueOf(record2.retryTimeUTC));
        }

        @Override
        public boolean equals(Object obj) {
            return false;
        }
    }
}

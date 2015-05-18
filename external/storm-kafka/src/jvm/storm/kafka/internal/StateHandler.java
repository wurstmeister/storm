package storm.kafka.internal;

import backtype.storm.Config;
import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.Partition;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkState;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StateHandler implements Serializable{

    public static final Logger LOG = LoggerFactory.getLogger(StateHandler.class);

    private SpoutConfig spoutConfig;
    private ZkState state;

    public void prepare(Map conf, SpoutConfig spoutConfig) {
        this.spoutConfig = spoutConfig;
        Map stateConf = new HashMap(conf);
        List<String> zkServers = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
        int zkPort = ((Number) conf.get(Config.STORM_ZOOKEEPER_PORT)).intValue();
        stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_SERVERS, zkServers);
        stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_PORT, zkPort);
        stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_ROOT, spoutConfig.zkRoot);
        state = new ZkState(stateConf);
    }

    public Long lastOffset(Partition topicPartition, String topologyInstanceId) {
        Long committedTo = spoutConfig.startOffsetTime;
        String jsonTopologyId = null;
        Long jsonOffset = null;
        String path = committedPath(topicPartition.partition);
        try {
            Map<Object, Object> json = state.readJSON(path);
            LOG.info("Read partition information from: " + path + "  --> " + json);
            if (json != null) {
                jsonTopologyId = (String) ((Map<Object, Object>) json.get("topology")).get("id");
                jsonOffset = (Long) json.get("offset");
            }
        } catch (Throwable e) {
            LOG.warn("Error reading and/or parsing at ZkNode: " + path, e);
        }
        if (jsonTopologyId == null || jsonOffset == null) { // failed to parse JSON?
            LOG.info("No partition information found, using configuration to determine offset");
        } else {
            committedTo = jsonOffset;
            LOG.info("Read last commit offset from zookeeper: " + committedTo + "; old topology_id: " + jsonTopologyId + " - new topology_id: " + topologyInstanceId);
        }
        return committedTo;
    }

    public void persist(Partition topicPartition, String topic, Long commitOffset, String _topologyInstanceId, String topologyName) {
        int _partition = topicPartition.partition;
        LOG.debug("Writing last completed offset (" + commitOffset + ") to ZK for " + _partition + " for topology: " + _topologyInstanceId);
        Map<Object, Object> data = (Map<Object, Object>) ImmutableMap.builder()
                .put("topology", ImmutableMap.of("id", _topologyInstanceId,
                        "name", topologyName))
                .put("offset", commitOffset)
                .put("partition", _partition)
                .put("topic", topic).build();
        state.writeJSON(committedPath(_partition), data);
        LOG.debug("Wrote last completed offset (" + commitOffset + ") to ZK for " + _partition + " for topology: " + _topologyInstanceId);

    }

    private String committedPath(int partition) {
        return spoutConfig.zkRoot + "/" + spoutConfig.id + "/" + partition;
    }

    public void close() {
        state.close();
    }
}

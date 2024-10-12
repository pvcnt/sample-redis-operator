package io.javaoperatorsdk.operator.sample.customresource;

import io.fabric8.kubernetes.model.annotation.StatusReplicas;
import io.javaoperatorsdk.operator.api.ObservedGenerationAwareStatus;

public class RedisClusterStatus extends ObservedGenerationAwareStatus {
    @StatusReplicas
    private Integer replicas;
    private Integer knownNodes;
    private Integer usedNodes;

    public Integer getReplicas() {
        return replicas;
    }

    public void setReplicas(Integer replicas) {
        this.replicas = replicas;
    }

    public Integer getKnownNodes() {
        return knownNodes;
    }

    public void setKnownNodes(Integer knownNodes) {
        this.knownNodes = knownNodes;
    }

    public void incrementKnownNodes() {
        this.knownNodes = (knownNodes == null) ? 1 : knownNodes + 1;
    }

    public void decrementKnownNodes() {
        this.knownNodes = (knownNodes == null) ? 0 : knownNodes - 1;
    }

    public Integer getUsedNodes() {
        return usedNodes;
    }

    public void setUsedNodes(Integer usedNodes) {
        this.usedNodes = usedNodes;
    }

    public void decrementUsedNodes() {
        this.usedNodes = (usedNodes == null) ? 0 : usedNodes - 1;
    }

    @Override
    public String toString() {
        return "RedisClusterStatus{" +
                "replicas=" + replicas +
                ", knownNodes=" + knownNodes +
                ", usedNodes=" + usedNodes +
                '}';
    }
}

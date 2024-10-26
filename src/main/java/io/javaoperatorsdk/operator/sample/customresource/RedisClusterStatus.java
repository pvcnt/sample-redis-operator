package io.javaoperatorsdk.operator.sample.customresource;

import io.fabric8.kubernetes.model.annotation.StatusReplicas;
import io.javaoperatorsdk.operator.api.ObservedGenerationAwareStatus;

public class RedisClusterStatus extends ObservedGenerationAwareStatus {
    @StatusReplicas
    private Integer replicas;

    public Integer getReplicas() {
        return replicas;
    }

    public void setReplicas(Integer replicas) {
        this.replicas = replicas;
    }

    @Override
    public String toString() {
        return "RedisClusterStatus{replicas=" + replicas + "}";
    }
}

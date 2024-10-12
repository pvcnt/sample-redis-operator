package io.javaoperatorsdk.operator.sample.customresource;

import io.fabric8.kubernetes.model.annotation.SpecReplicas;

public class RedisClusterSpec {
    private String version;
    @SpecReplicas
    private Integer replicas;

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public Integer getReplicas() {
        return replicas;
    }

    public void setReplicas(int replicas) {
        this.replicas = replicas;
    }

    @Override
    public String toString() {
        return "RedisClusterSpec{" +
                "version='" + version + '\'' +
                ", replicas=" + replicas +
                '}';
    }
}
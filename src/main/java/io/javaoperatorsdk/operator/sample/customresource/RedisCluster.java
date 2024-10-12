package io.javaoperatorsdk.operator.sample.customresource;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Version;

@Group("sample.javaoperatorsdk")
@Version("v1")
public class RedisCluster extends CustomResource<RedisClusterSpec, RedisClusterStatus> implements Namespaced {
    @Override
    public String toString() {
        return "RedisCluster{" +
                "spec=" + spec +
                ", status=" + status +
                '}';
    }
}

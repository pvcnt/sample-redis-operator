package io.javaoperatorsdk.operator.sample.dependentresource;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.sample.customresource.RedisCluster;

import java.util.Map;
import java.util.stream.Collectors;

import static io.javaoperatorsdk.operator.sample.RedisReconciler.MANAGED_LABEL;

public class ConfigMapDependentResource extends CRUDKubernetesDependentResource<ConfigMap, RedisCluster> {
    public static final int PORT = 6379;

    public static String configMapName(RedisCluster redisCluster) {
        return redisCluster.getMetadata().getName() + "-config";
    }

    public ConfigMapDependentResource() {
        super(ConfigMap.class);
    }

    @Override
    protected ConfigMap desired(RedisCluster redisCluster, Context<RedisCluster> context) {
        var config = Map.of(
                "cluster-enabled", "yes",
                "cluster-require-full-coverage", "no",
                "cluster-node-timeout", "5000",
                "cluster-config-file", "/data/nodes.conf",
                "appendonly", "yes",
                "protected-mode", "no",
                "bind", "0.0.0.0",
                "port", String.valueOf(PORT)
        );
        var data = Map.of("redis.conf", config.entrySet().stream().map(e -> e.getKey() + " " + e.getValue()).collect(Collectors.joining("\n")));
        return new ConfigMapBuilder()
                .withMetadata(
                        new ObjectMetaBuilder()
                                .withName(configMapName(redisCluster))
                                .withNamespace(redisCluster.getMetadata().getNamespace())
                                .withLabels(Map.of(MANAGED_LABEL, "true"))
                                .build())
                .withData(data)
                .build();
    }
}
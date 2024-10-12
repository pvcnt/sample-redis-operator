package io.javaoperatorsdk.operator.sample.dependentresource;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.sample.customresource.RedisCluster;

import static io.javaoperatorsdk.operator.sample.RedisReconciler.MANAGED_LABEL;
import static io.javaoperatorsdk.operator.sample.dependentresource.ConfigMapDependentResource.configMapName;

public class StatefulSetDependentResource extends CRUDKubernetesDependentResource<StatefulSet, RedisCluster> {
    public static final String CONTAINER_NAME = "redis";

    public static String statefulSetName(RedisCluster redisCluster) {
        return redisCluster.getMetadata().getName();
    }

    public StatefulSetDependentResource() {
        super(StatefulSet.class);
    }

    @Override
    protected StatefulSet desired(RedisCluster redisCluster, Context<RedisCluster> context) {
        var stsName = statefulSetName(redisCluster);
        return new StatefulSetBuilder()
                .editOrNewMetadata()
                .withName(stsName)
                .withNamespace(redisCluster.getMetadata().getNamespace())
                .addToLabels(MANAGED_LABEL, "true")
                .endMetadata()
                .withNewSpec()
                .withNewSelector()
                .addToMatchLabels("app", stsName)
                .endSelector()
                .withReplicas(redisCluster.getSpec().getReplicas())
                .withNewTemplate()
                .withNewMetadata()
                .addToLabels("app", stsName)
                .addToLabels(MANAGED_LABEL, "true")
                .endMetadata()
                .withNewSpec()
                .addNewContainer()
                .withName(CONTAINER_NAME)
                .withImage("redis:" + redisCluster.getSpec().getVersion())
                .withCommand("redis-server")
                .withArgs("/conf/redis.conf")
                .addNewEnv()
                .withName("REDIS_CLUSTER_ANNOUNCE_IP")
                .withNewValueFrom()
                .withNewFieldRef()
                .withFieldPath("status.podIP")
                .endFieldRef()
                .endValueFrom()
                .endEnv()
                .addNewPort()
                .withContainerPort(ConfigMapDependentResource.PORT)
                .withName("client")
                .endPort()
                .addNewPort()
                .withContainerPort(16379)
                .withName("gossip")
                .endPort()
                .addNewVolumeMount()
                .withName("data")
                .withMountPath("/data")
                .endVolumeMount()
                .addNewVolumeMount()
                .withName("config")
                .withMountPath("/conf")
                .endVolumeMount()
                .endContainer()
                .addNewVolume()
                .withName("config")
                .withNewConfigMap()
                .withName(configMapName(redisCluster))
                .endConfigMap()
                .endVolume()
                .addNewVolume()
                .withName("data")
                .withNewEmptyDir()
                .endEmptyDir()
                .endVolume()
                .endSpec()
                .endTemplate()
                .endSpec()
                .build();
    }
}

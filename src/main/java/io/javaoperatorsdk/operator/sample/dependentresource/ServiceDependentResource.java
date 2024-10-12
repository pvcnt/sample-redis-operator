package io.javaoperatorsdk.operator.sample.dependentresource;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.sample.customresource.RedisCluster;

import java.util.Map;

import static io.javaoperatorsdk.operator.sample.RedisReconciler.MANAGED_LABEL;
import static io.javaoperatorsdk.operator.sample.dependentresource.StatefulSetDependentResource.statefulSetName;

public class ServiceDependentResource extends CRUDKubernetesDependentResource<Service, RedisCluster> {
    public static String serviceName(RedisCluster redisCluster) {
        return redisCluster.getMetadata().getName();
    }

    public ServiceDependentResource() {
        super(Service.class);
    }

    @Override
    protected Service desired(RedisCluster redisCluster, Context<RedisCluster> context) {
        return new ServiceBuilder()
                .editOrNewMetadata()
                .withName(serviceName(redisCluster))
                .withNamespace(redisCluster.getMetadata().getNamespace())
                .addToLabels(MANAGED_LABEL, "true")
                .endMetadata()
                .withNewSpec()
                .withSelector(Map.of("app", statefulSetName(redisCluster)))
                .addNewPort()
                .withProtocol("TCP")
                .withPort(ConfigMapDependentResource.PORT)
                .withNewTargetPort()
                .withValue(ConfigMapDependentResource.PORT)
                .endTargetPort()
                .endPort()
                .endSpec()
                .build();
    }
}

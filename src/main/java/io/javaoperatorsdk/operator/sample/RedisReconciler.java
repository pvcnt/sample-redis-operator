package io.javaoperatorsdk.operator.sample;

import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.*;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.sample.customresource.RedisCluster;
import io.javaoperatorsdk.operator.sample.customresource.RedisClusterStatus;
import io.javaoperatorsdk.operator.sample.dependentresource.ConfigMapDependentResource;
import io.javaoperatorsdk.operator.sample.dependentresource.ServiceDependentResource;
import io.javaoperatorsdk.operator.sample.dependentresource.StatefulSetDependentResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static io.javaoperatorsdk.operator.sample.dependentresource.StatefulSetDependentResource.CONTAINER_NAME;

@ControllerConfiguration
public class RedisReconciler implements Reconciler<RedisCluster>, EventSourceInitializer<RedisCluster> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisReconciler.class);
    private static final Duration RESCHEDULE_AFTER = Duration.ofSeconds(30);
    private static final String POD_INDEX_LABEL = "apps.kubernetes.io/pod-index";
    public static final String MANAGED_LABEL = "redis-operator.io/managed";

    private final KubernetesDependentResource<ConfigMap, RedisCluster> configMapDR = new ConfigMapDependentResource();
    private final KubernetesDependentResource<StatefulSet, RedisCluster> statefulSetDR = new StatefulSetDependentResource();
    private final KubernetesDependentResource<Service, RedisCluster> serviceDR = new ServiceDependentResource();

    @Override
    public Map<String, EventSource> prepareEventSources(EventSourceContext<RedisCluster> context) {
        return EventSourceInitializer.nameEventSourcesFromDependentResource(context, configMapDR, statefulSetDR, serviceDR);
    }

    @Override
    public UpdateControl<RedisCluster> reconcile(RedisCluster redisCluster, Context<RedisCluster> context) {
        if (null == redisCluster.getStatus()) {
            redisCluster.setStatus(new RedisClusterStatus());
        }

        var maybeSts = context.getSecondaryResource(StatefulSet.class);
        if (maybeSts.isPresent() && isStable(maybeSts.get())) {
            if (redisCluster.getStatus().getKnownNodes() != null && redisCluster.getStatus().getKnownNodes() > redisCluster.getSpec().getReplicas()) {
                if (!scaleDown(redisCluster, maybeSts.get(), context)) {
                    LOGGER.error("Error while scaling down, rescheduling");
                    return UpdateControl.patchStatus(redisCluster).rescheduleAfter(RESCHEDULE_AFTER);
                }
            }
        }

        configMapDR.reconcile(redisCluster, context);
        statefulSetDR.reconcile(redisCluster, context);
        serviceDR.reconcile(redisCluster, context);

        var sts = context.getSecondaryResource(StatefulSet.class).orElseThrow();

        if (isStable(redisCluster, sts)) {
            if (redisCluster.getStatus().getKnownNodes() == null || redisCluster.getStatus().getKnownNodes() == 0) {
                return createCluster(redisCluster, sts, context);
            } else if (redisCluster.getStatus().getUsedNodes() == null || redisCluster.getStatus().getUsedNodes() < redisCluster.getSpec().getReplicas()) {
                return scaleUp(redisCluster, sts, context);
            }
        }
        return UpdateControl.patchStatus(redisCluster);
    }

    private UpdateControl<RedisCluster> createCluster(RedisCluster redisCluster, StatefulSet sts, Context<RedisCluster> context) {
        LOGGER.info("Creating a new cluster with {} nodes", sts.getStatus().getReplicas());

        var pods = getPods(sts, context);
        var args = new ArrayList<>(List.of("redis-cli", "--cluster", "create"));
        pods.forEach(pod -> args.add(pod.getStatus().getPodIP() + ":" + ConfigMapDependentResource.PORT));
        args.add("--cluster-yes");

        var res = exec(context.getClient(), pods.get(0), args);
        if (res.exitCode() == 0) {
            LOGGER.info("Created a cluster with {} nodes", sts.getStatus().getReplicas());
            redisCluster.getStatus().setKnownNodes(redisCluster.getSpec().getReplicas());
            redisCluster.getStatus().setUsedNodes(redisCluster.getSpec().getReplicas());

            return UpdateControl.patchStatus(redisCluster);
        } else {
            LOGGER.error("Could not create cluster [{}]: {}", res.exitCode(), res.out());
            return UpdateControl.<RedisCluster>noUpdate().rescheduleAfter(RESCHEDULE_AFTER);
        }
    }

    private UpdateControl<RedisCluster> scaleUp(RedisCluster redisCluster, StatefulSet sts, Context<RedisCluster> context) {
        return scaleUp(redisCluster, sts, context, true);
    }

    private UpdateControl<RedisCluster> scaleUp(RedisCluster redisCluster, StatefulSet sts, Context<RedisCluster> context, boolean tryFix) {
        LOGGER.info("Scaling cluster up {} -> {}", redisCluster.getStatus().getUsedNodes(), redisCluster.getSpec().getReplicas());

        var pods = getPods(sts, context);
        for (int i = redisCluster.getStatus().getKnownNodes(); i < sts.getStatus().getReplicas(); i++) {
            var pod = pods.get(i);
            var args = List.of("redis-cli", "--cluster", "add-node", getHostAndPort(pod), getHostAndPort(pods.get(0)), "--cluster-yes");
            var res = exec(context.getClient(), pods.get(0), args);
            if (res.exitCode() == 0) {
                LOGGER.info("Added node {} to the cluster", pod.getMetadata().getName());
                redisCluster.getStatus().incrementKnownNodes();
            } else {
                LOGGER.error("Could not add node {} to the cluster [{}]: {}", pod.getMetadata().getName(), res.exitCode(), res.out());
                return UpdateControl.patchStatus(redisCluster).rescheduleAfter(RESCHEDULE_AFTER);
            }
        }

        var args = List.of("redis-cli", "--cluster", "rebalance", getHostAndPort(pods.get(0)), "--cluster-use-empty-masters", "--cluster-yes");
        var res = exec(context.getClient(), pods.get(0), args);
        if (res.exitCode() == 0) {
            redisCluster.getStatus().setUsedNodes(redisCluster.getStatus().getKnownNodes());
            LOGGER.info("Scaled the cluster to {} nodes", sts.getStatus().getReplicas());
            return UpdateControl.patchStatus(redisCluster);
        } else {
            LOGGER.error("Could not rebalance cluster [{}]: {}", res.exitCode(), res.out());
            if (tryFix && fixCluster(context.getClient(), pods.get(0))) {
                return scaleUp(redisCluster, sts, context, false);
            }
            return UpdateControl.patchStatus(redisCluster).rescheduleAfter(RESCHEDULE_AFTER);
        }
    }

    private boolean fixCluster(KubernetesClient client, Pod pod) {
        var args = List.of("redis-cli", "--cluster", "fix", getHostAndPort(pod));
        var res = exec(client, pod, args);
        if (res.exitCode() == 0) {
            LOGGER.info("Fixed the cluster");
            return true;
        } else {
            LOGGER.error("Could not fix the cluster [{}]: {}", res.exitCode(), res.out());
            return false;
        }
    }

    private boolean scaleDown(RedisCluster redisCluster, StatefulSet sts, Context<RedisCluster> context) {
        LOGGER.info("Scaling cluster down {} -> {}", redisCluster.getStatus().getKnownNodes(), redisCluster.getSpec().getReplicas());

        var pods = getPods(sts, context);
        var nodes = getNodes(context.getClient(), pods);
        assert pods.size() == nodes.size();
        for (int i = redisCluster.getSpec().getReplicas(); i < redisCluster.getStatus().getKnownNodes(); i++) {
            var pod = pods.get(i);
            var node = nodes.get(i);

            if (node.numSlots > 0) {
                var targetNode = nodes.get(i % redisCluster.getSpec().getReplicas());
                var args = List.of("redis-cli", "--cluster", "reshard", getHostAndPort(pods.get(0)),
                        "--cluster-from", node.nodeId,
                        "--cluster-to", targetNode.nodeId,
                        "--cluster-slots", String.valueOf(node.numSlots),
                        "--cluster-yes");
                var res = exec(context.getClient(), pods.get(0), args);
                if (res.exitCode() == 0) {
                    redisCluster.getStatus().decrementUsedNodes();
                    LOGGER.info("Resharded cluster from {} to {}", node.nodeId, targetNode.nodeId);
                } else {
                    LOGGER.error("Could not reshard cluster from {} to {} [{}]: {}", node.nodeId, targetNode.nodeId, res.exitCode(), res.out());
                    return false;
                }
            }

            var args = List.of("redis-cli", "--cluster", "del-node", getHostAndPort(pods.get(0)), node.nodeId(), "--cluster-yes");
            var res = exec(context.getClient(), pods.get(0), args);
            if (res.exitCode() == 0) {
                redisCluster.getStatus().decrementKnownNodes();
                LOGGER.info("Removed node {} from the cluster", pod.getMetadata().getName());
            } else {
                LOGGER.error("Could not remove node {} from the cluster [{}]: {}", pod.getMetadata().getName(), res.exitCode(), res.out());
                return false;
            }
        }
        return true;
    }

    private static List<Pod> getPods(StatefulSet sts, Context<RedisCluster> context) {
        return context.getClient().pods()
                .inNamespace(sts.getMetadata().getNamespace())
                .withLabel(MANAGED_LABEL, "true")
                .list()
                .getItems()
                .stream()
                .filter(pod -> isOwnedBy(pod, sts))
                .sorted(Comparator.comparing(RedisReconciler::getOrdinal))
                .toList();
    }

    private static String getHostAndPort(Pod pod) {
        return pod.getStatus().getPodIP() + ":" + ConfigMapDependentResource.PORT;
    }

    private static Integer getOrdinal(Pod pod) {
        return Integer.parseInt(pod.getMetadata().getLabels().get(POD_INDEX_LABEL));
    }

    private static boolean isOwnedBy(Pod pod, StatefulSet sts) {
        return pod.getMetadata().getOwnerReferences() != null && pod.getMetadata()
                .getOwnerReferences()
                .stream()
                .anyMatch(ref -> ref.getController() && ref.getUid().equals(sts.getMetadata().getUid()));
    }

    private static boolean isStable(RedisCluster redisCluster, StatefulSet sts) {
        return isStable(sts) && Objects.equals(redisCluster.getSpec().getReplicas(), sts.getStatus().getAvailableReplicas());
    }

    private static boolean isStable(StatefulSet sts) {
        return sts.getStatus() != null
                && Objects.equals(sts.getMetadata().getGeneration(), sts.getStatus().getObservedGeneration())
                && Objects.equals(sts.getSpec().getReplicas(), sts.getStatus().getAvailableReplicas());
    }

    private record ExecResult(int exitCode, String out) {
    }

    private static ExecResult exec(KubernetesClient client, Pod pod, List<String> args) {
        var stopwatch = Stopwatch.createStarted();
        var baos = new ByteArrayOutputStream();
        try (var watch = client.pods()
                .resource(pod)
                .inContainer(CONTAINER_NAME)
                .writingOutput(baos)
                .writingError(baos)
                .exec(args.toArray(new String[0]))) {
            int exitCode = watch.exitCode().get(2, TimeUnit.MINUTES);
            LOGGER.info("Executed [{}] on {} in {}", String.join(" ", args), pod.getMetadata().getName(), stopwatch);
            return new ExecResult(exitCode, baos.toString());
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            LOGGER.error("Error while executing [{}] on {}", String.join(" ", args), pod.getMetadata().getName(), e);
            return new ExecResult(-1, e.toString());
        }
    }

    private record Node(String nodeId, String hostAndPort, int numSlots) {
    }

    private List<Node> getNodes(KubernetesClient client, List<Pod> pods) {
        var args = List.of("redis-cli", "cluster", "nodes");
        var res = exec(client, pods.get(0), args);
        if (res.exitCode() == 0) {
            var ordinalByHostAndPort = pods.stream().collect(Collectors.toMap(RedisReconciler::getHostAndPort, RedisReconciler::getOrdinal));
            return Splitter.on("\n")
                    .trimResults()
                    .omitEmptyStrings()
                    .splitToStream(res.out())
                    .map(this::parseNode)
                    .sorted(Comparator.comparing(node -> ordinalByHostAndPort.getOrDefault(node.hostAndPort, Integer.MAX_VALUE)))
                    .toList();
        } else {
            LOGGER.error("Could not obtain nodes [{}]: {}", res.exitCode(), res.out());
            return List.of();
        }
    }

    private Node parseNode(String line) {
        // 5592b02ba4a6ec5e22e65c01235b4df40603022b 10.1.1.32:6379@16379 master - 0 1728848005049 7 connected 10381-10766 13654-14043
        String[] parts = line.split(" ");
        String hostAndPort = parts[1].split("@")[0];
        int numSlots = 0;
        for (var i = 8; i < parts.length; i++) {
            String[] slotsRange = parts[i].split("-");
            numSlots += Integer.parseInt(slotsRange[1]) - Integer.parseInt(slotsRange[0]) + 1;
        }
        return new Node(parts[0], hostAndPort, numSlots);
    }
}

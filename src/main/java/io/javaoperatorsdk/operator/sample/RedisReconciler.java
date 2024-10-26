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
    private static final Duration RESCHEDULE_AFTER = Duration.ofSeconds(15);
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
            // status is null by default. Initialize it so that it can be manipulated later.
            // It is also required for automatic observed generation handling.
            // https://javaoperatorsdk.io/docs/features/#automatic-observed-generation-handling
            redisCluster.setStatus(new RedisClusterStatus());
        }

        // 1/ We verify whether we need to scale down the cluster. This must be done
        // before the StatefulSet is reconciled, to ensure that we do not lose any
        // data stored in a cluster node.
        var maybeSts = context.getSecondaryResource(StatefulSet.class);
        if (maybeSts.isPresent()) {
            var pods = getPods(maybeSts.get(), context);
            var nodes = getNodes(context.getClient(), pods);
            if (nodes.size() > redisCluster.getSpec().getReplicas()) {
                LOGGER.info("Scaling cluster down {} -> {}", nodes.size(), redisCluster.getSpec().getReplicas());
                if (!pods.stream().allMatch(RedisReconciler::isHealthy)) {
                    LOGGER.info("Pods are not healthy, skipping");
                    return UpdateControl.patchStatus(redisCluster);
                }
                if (!scaleDown(redisCluster, nodes, pods, context)) {
                    LOGGER.error("Error while scaling down, rescheduling");
                    return patchStatusAndReschedule(redisCluster);
                }
            }
        }

        // 2/ Propagate any updates made to a RedisCluster to dependent resources.
        configMapDR.reconcile(redisCluster, context);
        statefulSetDR.reconcile(redisCluster, context);
        serviceDR.reconcile(redisCluster, context);

        // 3/ Retrieve the StatefulSet that was created or updated, all the pods and
        // nodes that form the cluster.
        var sts = context.getSecondaryResource(StatefulSet.class).orElseThrow();
        var pods = getPods(sts, context);
        redisCluster.getStatus().setReplicas(pods.size());
        if (pods.size() != sts.getStatus().getReplicas()) {
            // This is a race condition.
            LOGGER.warn("Pods do not match expected replicas (got {}, expected {})", pods.size(), sts.getStatus().getReplicas());
            return patchStatusAndReschedule(redisCluster);
        }
        if (!pods.stream().allMatch(RedisReconciler::isHealthy)) {
            LOGGER.info("Pods are not healthy, skipping");
            return UpdateControl.patchStatus(redisCluster);
        }
        var nodes = getNodes(context.getClient(), pods);
        if (nodes.size() == 1) {
            // A Redis cluster needs a minimum of three nodes. If we have a single node,
            // it means it is a newly created cluster.
            if (!createCluster(redisCluster, pods, context)) {
                return patchStatusAndReschedule(redisCluster);
            }
        } else {
            if (nodes.size() < pods.size()) {
                LOGGER.info("Scaling cluster up {} -> {}", nodes.size(), pods.size());
                if (!scaleUp(redisCluster, nodes, pods, context)) {
                    return patchStatusAndReschedule(redisCluster);
                }
            }
            nodes = getNodes(context.getClient(), pods);
            if (nodes.stream().anyMatch(node -> node.numSlots == 0)) {
                LOGGER.info("Rebalancing the cluster with {} nodes", pods.size());
                if (!rebalance(redisCluster, pods, context)) {
                    return patchStatusAndReschedule(redisCluster);
                }
            }
        }
        // Everything went well, we simply patch the status.
        return UpdateControl.patchStatus(redisCluster);
    }

    private boolean createCluster(RedisCluster redisCluster, List<Pod> pods, Context<RedisCluster> context) {
        LOGGER.info("Creating a new cluster with {} nodes", pods.size());

        var args = new ArrayList<>(List.of("redis-cli", "--cluster-yes", "--cluster", "create"));
        pods.forEach(pod -> args.add(pod.getStatus().getPodIP() + ":" + ConfigMapDependentResource.PORT));

        var res = exec(context.getClient(), pods.get(0), args);
        if (res.exitCode() == 0) {
            LOGGER.info("Created a cluster with {} nodes", pods.size());
            redisCluster.getStatus().setKnownNodes(redisCluster.getSpec().getReplicas());
            redisCluster.getStatus().setUsedNodes(redisCluster.getSpec().getReplicas());

            return true;
        } else {
            LOGGER.error("Could not create cluster [{}]: {}", res.exitCode(), res.out());
            return false;
        }
    }

    private boolean scaleUp(RedisCluster redisCluster, List<Node> nodes, List<Pod> pods, Context<RedisCluster> context) {
        for (int i = redisCluster.getStatus().getKnownNodes(); i < pods.size(); i++) {
            var pod = pods.get(i);
            var args = List.of("redis-cli", "--cluster-yes", "--cluster", "add-node", getHostAndPort(pod), getHostAndPort(pods.get(0)));
            var res = exec(context.getClient(), pods.get(0), args);
            if (res.exitCode() == 0) {
                LOGGER.info("Added node {} to the cluster", pod.getMetadata().getName());
                redisCluster.getStatus().incrementKnownNodes();
            } else {
                LOGGER.error("Could not add node {} to the cluster [{}]: {}", pod.getMetadata().getName(), res.exitCode(), res.out());
                fixCluster(context.getClient(), pods);
                return false;
            }
        }
        return true;
    }

    private boolean rebalance(RedisCluster redisCluster, List<Pod> pods, Context<RedisCluster> context) {
        var args = List.of("redis-cli", "--cluster-yes", "--cluster", "rebalance", getHostAndPort(pods.get(0)), "--cluster-use-empty-masters");
        var stopWatch = Stopwatch.createStarted();
        var res = exec(context.getClient(), pods.get(0), args);
        if (res.exitCode() == 0) {
            redisCluster.getStatus().setUsedNodes(redisCluster.getStatus().getKnownNodes());
            LOGGER.info("Rebalanced the cluster with {} nodes in {}", pods.size(), stopWatch);
            return true;
        } else {
            LOGGER.error("Could not rebalance cluster [{}]: {}", res.exitCode(), res.out());
            fixCluster(context.getClient(), pods);
            return false;
        }
    }

    private void fixCluster(KubernetesClient client, List<Pod> pods) {
        LOGGER.info("Fixing the cluster with {} nodes", pods.size());
        var args = List.of("redis-cli", "--cluster-yes", "--cluster", "fix", getHostAndPort(pods.get(0)));
        var stopWatch = Stopwatch.createStarted();
        var res = exec(client, pods.get(0), args);
        if (res.exitCode() == 0) {
            LOGGER.info("Fixed the cluster with {} nodes in {}", pods.size(), stopWatch);
        } else {
            LOGGER.error("Could not fix the cluster [{}]: {}", res.exitCode(), res.out());
        }
    }

    private boolean scaleDown(RedisCluster redisCluster, List<Node> nodes, List<Pod> pods, Context<RedisCluster> context) {
        for (int i = redisCluster.getSpec().getReplicas(); i < nodes.size(); i++) {
            var node = nodes.get(i);
            if (node.numSlots > 0) {
                var targetNode = nodes.get(i % redisCluster.getSpec().getReplicas());
                LOGGER.info("Resharding cluster from {} to {}", node.nodeId, targetNode.nodeId);
                var args = List.of("redis-cli", "--cluster-yes",
                        "--cluster", "reshard", getHostAndPort(pods.get(0)),
                        "--cluster-from", node.nodeId,
                        "--cluster-to", targetNode.nodeId,
                        "--cluster-slots", String.valueOf(node.numSlots));
                var stopWatch = Stopwatch.createStarted();
                var res = exec(context.getClient(), pods.get(0), args);
                if (res.exitCode() == 0) {
                    redisCluster.getStatus().decrementUsedNodes();
                    LOGGER.info("Resharded cluster from {} to {} in {}", node.nodeId, targetNode.nodeId, stopWatch);
                } else {
                    LOGGER.error("Could not reshard cluster from {} to {} [{}]: {}", node.nodeId, targetNode.nodeId, res.exitCode(), res.out());
                    return false;
                }
            }

            LOGGER.info("Removing node {} from the cluster", node.nodeId);
            var args = List.of("redis-cli", "--cluster-yes", "--cluster", "del-node", getHostAndPort(pods.get(0)), node.nodeId());
            var stopWatch = Stopwatch.createStarted();
            var res = exec(context.getClient(), pods.get(0), args);
            if (res.exitCode() == 0) {
                redisCluster.getStatus().decrementKnownNodes();
                LOGGER.info("Removed node {} from the cluster in {}", node.nodeId, stopWatch);
            } else {
                LOGGER.error("Could not remove node {} from the cluster [{}]: {}", node.nodeId, res.exitCode(), res.out());
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

    private static boolean isHealthy(Pod pod) {
        return pod.getStatus() != null
                && pod.getStatus().getConditions().stream().anyMatch(c -> "Ready".equals(c.getType()) && "True".equals(c.getStatus()))
                && pod.getMetadata().getDeletionTimestamp() == null;
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
        // https://redis.io/docs/latest/commands/cluster-nodes/
        // <id> <ip:port@cport[,hostname]> <flags> <master> <ping-sent> <pong-recv> <config-epoch> <link-state> <slot> <slot> ... <slot>
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

    private UpdateControl<RedisCluster> patchStatusAndReschedule(RedisCluster redisCluster) {
        return UpdateControl.patchStatus(redisCluster).rescheduleAfter(RESCHEDULE_AFTER);
    }
}

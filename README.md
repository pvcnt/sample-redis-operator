# Redis Operator

This is an example of a [Kubernetes operator](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/), implemented using the [Java Operator SDK](https://javaoperatorsdk.io/), that deploys Redis clusters.
It showcases how an operator can decrease the cognitive workload of an engineer by automating the deployment and scaling of a stateful service. 

**This operator is only for educational purposes, it should not be used in production!**

## Build and test locally

Build the operator (requires Java 17): 
```
mvn compile package
```

Deploy the CRD:
```
kubectl apply -f target/classes/META-INF/fabric8/redisclusters.sample.javaoperatorsdk-v1.yml
```

Deploy the operator:
```
kubectl apply -f k8s/operator.yaml
```

Deploy the cluster:
```
kubectl apply -f k8s/redis-cluster.yaml
```

Validate the cluster:
```
kubectl exec redis-cluster-0 -- redis-cli --cluster check localhost:6379
```

Scale the cluster:
```
kubectl scale --replicas=4 redisclusters/redis-cluster
```
### Reminder: Important Kubernetes objects

___

* Pod
    * Group of one or more containers with shared storage/network and specification for how to run containers
* Replica Set
    * Ensures that a specified number of pod replicas are running at any given time.
* Deployment
    * Describes a desired state and a deployment controller makes sure that the state is maintained.
* Service
    * Defines a logical set of Pods and a policy by which to access them.
* Storage class
    * Provides a way to describe a "class" of storage. Represents a Persistent Volume.
* Persistent Volume Claim
    * Abstraction of Persistent Volumes. A Persistent volumes is a piece of storage in the cluster.

### Drawbacks of the Celery Executor

___

* Need to set up tier applications (`RabbitMQ`, `Celery`,`Flower`)
* Deal with missing dependencies
* Wasting resources: airflow workers stay idle when no workload
* Worker nodes are not as resilient as you think

### What is the Kubernetes Executor?

___

* Runs your tasks on Kubernetes
* One task = One Pod
* Task-level Pod configuration
* Expands and shrinks your cluster according to the workload
* Pods run to completion
* Scheduler subscribes to Kubernetes event stream
* Dag distribution:
    * Git clone with init-container for each Pod
    * Mount volume with DAGs
    * Build image with the DAG codes

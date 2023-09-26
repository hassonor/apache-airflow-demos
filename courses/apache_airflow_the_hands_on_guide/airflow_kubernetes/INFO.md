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
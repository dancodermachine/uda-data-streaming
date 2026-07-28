# Apache Kafka
## 1. Kafka Architecture
* Kafka servers are referred to as brokers.
* All of the brokers that work together are referred to as a cluster.
* Clusters may consist of just one broker, or thousands of brokers.
Apache Zookeeper(opens in a new tab) was historically used by Kafka brokers to determine which broker is the leader of a given partition and topic, track cluster membership, and store configuration for topics and permissions (ACLs). As of Kafka 3.3, ZooKeeper has been deprecated and replaced by KRaft (Kafka Raft) mode, which allows Kafka to manage its own metadata without an external coordination service.
* ACLs are permissions associated with an object. In Kafka, this typically refers to a user's permissions with respect to production and consumption, and/or the topics themselves.
* Kafka nodes may gracefully join and leave the cluster.
* Kafka runs on the Java Virtual Machine (JVM).

## 2. How Kafka Stores Data


## Glossary


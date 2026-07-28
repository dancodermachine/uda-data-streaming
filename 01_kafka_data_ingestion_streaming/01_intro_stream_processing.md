# Intro to Streaming Processing
## 1. Intro
### 1.1 Understanding Stream Processing
Stream Processing is the act of performing continual calculations on a potentially endless and constantly evolving source of data.

Stream Processing applications perform calculations on Data Streams. Data Streams consist of a potentially endless stream of immutable data.

Immutable data does not change -- once the data has been placed in the data stream it can never be updated. Another data entry can be placed in the stream that supersedes the previous data entry if necessary.

Data sent to data streams is typically small, less than 1MB in size.

The data throughput to data streams is highly variable. Some streams will receive thousands or tens of thousands of records per second, and some will receive one or two records per hour.

Stream Processing acts on potentially endless and constantly evolving immutable data contained in data streams.

Once data have been placed in a data stream, they cannot be modified. We must place a new record in the stream to override the existing data.

Data in data streams can vary widely in size depending on the application and platform, and the data volume may vary from a few records an hour to thousands of requests per second.

### 1.2 What is an Event?
* **Event** – a fact regarding something that occurred within a system, typically immutable once created in event-driven architectures. Some systems may allow corrections or compensations via additional events. Data records in the context of data streaming are events.
In many SQL databases, it is uncommon to track the history of what values were used for a particular user in the past.
* **Message Queues** – typically communicate commands to perform an action
* **Evented Systems** – react to the facts that are indirectly communicated to them, for example, via user clicks

### 1.3 Examples
* **Log Analysis**: Companies often run microservices that constantly produce logs that are full of information (e.g., user behaviour patterns, failure prediction, debugging).
* **Web Analytics**: Modern web applications measure almost every action a user takes on their site (e.g., button clicks, page load times, session duration). 
* **Real-Time Pricing**

### 1.3 Streaming Data Store
* May look like a message queue, as is the case with Apache Kafka
* May look like a SQL store, as is the case with Apache Cassandra
* Responsible for holding all of the immutable event data in the system
* Provides guarantee that data is stored ordered according to the time it was produced
* Provides guarantee that data is produced to consumers in the order it was received
* Provides guarantee that the events it stores are immutable and unchangeable

## 2. Stream Processing Application and Framework
* Stream Processing applications sit downstream of the data store
* Stream Processing applications ingest real-time event data from one or more data streams
* Stream Processing applications aggregate, join, and find differences in data from these streams
* Common Stream Processing Application Frameworks in use today include:
    - ksqlDB
    - Kafka Streams
    - Apache Flink
    - Apache Samza
    - Apache Spark Structure Streaming
    - Faust Python Library

## 3. Benefits of Stream processing
* Faster for scenarios where a limited set of recent data is needed
More scalable due to distributed nature of storage
* Provides a useful abstraction that decouples applications from each other
* Allows one set of data to satisfy many use-cases which may not have been predictable when the dataset was originally created
* Built-in ability to replay events and observe exactly what occurred, and in what order, provides more opportunities to recover from error states or dig into how a particular result was arrived at

## 4 Append-Only Logs
* Append-only logs are text files in which incoming events are written to the end of the log as they are received.
* This simple concept -- of only ever appending, or adding, data to the end of a log file -- is what allows stream processing applications to ensure that events are ordered correctly even at high throughput and scale.
* We can take this idea a step farther, and say that in fact, streams are append-only logs.

## 5 Log-structured streaming
* Log-structured streams build upon the concept of append-only logs. One of the hallmarks of log-structured storage systems is that at their core they utilize append-only logs.
* Common characteristics of all log-structured storage systems are that they simply append data to log files on disk.
* These log files may store data indefinitely, for a specific time period, or until a specific size is reached.
* There are typically many log files on disk, and these log files are merged and compacted occasionally.
* When a log file is merged it means that two or more log files are joined together into one log file.
* When a log file is compacted it means that data from one or more files is deleted. Deletion is typically determined by the age of a record. The oldest records are removed, while the newest stay.
* Examples of real world log-structured data stores: Apache HBase, Apache Cassandra, Apache Kafka

**Log-Structured Storage**
* One of the key innovations over the past decade in computing has been the emergence of log-structured storage as a primary means of storing data.

SQL databases use append-only logs to track all creations, updates, and deletes made to the database so that those changes can be synced to replicas

## 6 Kafka
* Kafka is one of the most popular streaming data platforms in the industry today.
* Provides an easy-to-use message queue interface on top of its append-only log-structured storage medium
* Kafka is a log of events
* In Kafka, an event describes something that has occurred, as opposed to a request for an action to be performed
* Kafka is distributed by default
* Fault tolerant by design, meaning it is hard to lose data if a node is suddenly lost
* Kafka scales from 1 to thousands of nodes
* Kafka provides ordering guarantees for data stored within it, meaning that the order in which data is received is the order in which data will be produced to consumers
* Commonly used data store for popular streaming tools like Apache Spark, Flink, and Samza

### 6.1 Kafka Topic
* Used to organize and segment datasets, similar to SQL database tables
* Unlike SQL database tables, Kafka Topics are not queryable.
* May be created programmatically, from a CLI (Command Line Interface), or automatically
* Consist of key-value data in binary format
### 6.2 Kafka Producer
* Send event data into Kafka topics
* Integrate with client libraries in languages like Java, Python, Go, as well as many other languages
### 6.3 Kafka Consumer
* Pull event data from one or more Kafka Topics
* Integrate with Kafka via a Client Library written in languages like Python, Java, Go, and more
* By default, if a consumer group has no committed offsets, the consumer will start consuming from the latest offset (new messages). This behavior can be changed by configuring the `auto.offset.reset` property to `earliest` to consume historical data.

## 7. Glossary
* **Stream** - An unbounded sequence of ordered, immutable data
* **Stream Processing** - Continual calculations performed on one or more Streams
* **Immutable Data** - Data that cannot be changed once it has been created
* **Event** - An immutable fact regarding something that has occurred in our system.
* **Batch Processing** - Scheduled, periodic analysis of one or more groups of related data.
* **Data Store** - A repository for persistently storing and managing collections of data, such as databases, file systems, or object stores.
* **Stream Processing Application** - An application which is downstream of one or more data streams and performs some kind of calculation on incoming data, typically producing one or more output data streams
* **Stream Processing Framework** - A set of tools, typically bundled as a library, used to construct a Stream Processing Application
* **Real-time** - In relation to processing, this implies that a piece of data, or an event, is processed almost as soon as it is produced. Strict time-based definitions of real-time are controversial in the industry and vary widely between applications. For example, a Computer Vision application may consider real-time to be 1 millisecond or less, whereas a data engineering team may consider it to be 30 seconds or less. In this class when the term "real-time" is used, the time-frame we have in mind is seconds.
* **Append-only Log** - A data structure or storage abstraction where new records are appended sequentially, preserving the order of events and immutability of existing data.
* **Change Data Capture (CDC)** - The process of capturing change events, typically in SQL database systems, in order to accurately communicate and synchronize changes from primary to replica nodes in a clustered system.
* **Log-Structured Storage** - Systems built on Append-Only Logs, in which system data is stored in log format.
* **Merge (Log Files)** - When two or more log files are joined together into a single output log file
* **Compact (Log Files)** - When data from one or more files is deleted, typically based on the age of data
* **Source (Kafka)** - A term sometimes used to refer to Kafka clients which are producing data into Kafka, typically in reference to another data store
* **Sink (Kafka)** - A term sometimes used to refer to Kafka clients which are extracting data from Kafka, typically in reference to another data store
* **Topic (Kafka)** - A logical construct used to organize and segment datasets within Kafka, similar to how SQL databases use tables
* **Producer (Kafka)** - An application which is sending data to one or more Kafka Topics.
* **Consumer (Kafka)** - An application which is receiving data from one or more Kafka Topics.
# Java Kinesis Consumer

## Introduction

Java Kinesis consumer leverages the AWS SDK to interact with Amazon Kinesis and retrieve records using a pull model. It
is designed for resilience, automatically recovering from intermediate failures and seamlessly resuming where it left
off. The Consumer also efficiently deduplicates records that may appear within the same shard or across multiple shards.

## Getting Started

### Prerequisites

To run the Java Kinesis Consumer, you need the following dependencies installed on your system:

- [Redis](https://redis.io/docs/install/install-redis/)
- [Java](https://www.java.com/en/download/help/download_options.html)
- [Apache Maven (mvn)](https://maven.apache.org/download.cgi)

### Setup

1. **Configure AWS:** Set up your AWS credentials and configure them in your system. You can follow the official
   guide [here](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html).

2. **Start Redis Server:**
   Start the Redis server on your system. You can refer to the Redis documentation for instructions on how to do
   this: [Redis Installation](https://redis.io/docs/install/install-redis/install-redis-on-mac-os/#starting-and-stopping-redis-in-the-foreground).

3. **Clone the repo:**
    ```bash
    git clone https://github.com/pradheep-arul/kinesis-consumer.git
    cd kinesis-consumer
    ```

4. **Update Configuration:**
   Update the [redis.properties](src/main/resources/redis.properties) file with the correct Redis hostname and port if
   you are not using the default settings.

5. **Build the Project:**
   Use Maven to create the executable JAR file.

    ```bash
    mvn package
    ```

6. **Run the Java Consumer:**
   Start the Java Consumer using the following command, replacing `<jar_path>` with the path to the generated JAR file
   ideally  `target/kinesis-consumer-1.0-SNAPSHOT.jar`.

    ```bash
    java -jar <jar_path>
    ```

## High-Level Design

![High-Level Design](https://user-images.githubusercontent.com/30936633/279166241-59e91ca8-5d52-491f-b6b1-a74769556306.jpg)

At a high level, the Consumer operates by retrieving records from Kinesis using a pull model. It utilizes Redis to
manage the processing and processed records' hashes for efficient deduplication. The records are then persistently
stored in a disk location (with S3 as a preferred option in a production environment) for potential future batch
processing. The system performs real-time metrics computation on the streaming records and provides outputs in the
console, which can be further integrated into data warehouses or Datadog for dashboard visualization.

## Consumer Workflow

![Consumer Workflow](https://user-images.githubusercontent.com/30936633/279166128-32d0f2d7-6e61-4560-b73e-453382cae9ca.jpg)

- Consumer retrieves records from Kinesis.
- It decodes the messages and converts them into map objects.
- The raw data is stored in storage.
- It deduplicates the records.
- Deduplicated records are stored for future processing.
- Metrics are computed and logged in the console.

## Log files

- **raw-records-<time>.log**: This log file contains all the decoded records fetched from Kinesis. It is rotated every 5
  minutes
- **dedup-records-<time>.log**: This log file contains all the deduplicated records fetched from Kinesis. It is rotated
  every 5 minutes.
- **worker.log**: The worker log contains details about the shards fetched from Kinesis, the processing of each shard,
  and logs any duplicate records found in Kinesis.

## Scaling Opportunities

- **Multi-Threading:** Implement multi-threading to read each shard using a dedicated thread efficiently.
- **Multiple Worker Instances:** Run multiple instances of the Worker, with each instance responsible for processing a
  specific shard. This approach can help distribute the workload across multiple consumers.

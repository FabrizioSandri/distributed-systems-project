# Distributed Key-Value Store in Akka

## Overview

Inspired by Amazon Dynamo, this project introduces a DHT-based peer-to-peer
key-value storage service implemented using Akka actors. It provides a simple
user interface to upload/request data and issue management commands, emphasizing
key principles of data partitioning, replication, and dynamic network
management.

## Features

- **Data Partitioning and Replication**: Efficiently balances the load among
  nodes based on keys, ensuring data redundancy and resilience.
- **Consistency Maintenance**: Employs quorums and versioning to maintain data
  consistency while accommodating read and write operations.
- **Dynamic Network Management**: Gracefully handles dynamic network changes
  such as node joins and leaves, ensuring seamless data repartitioning.

## Implementation

The solution is designed around the actor-based Akka framework. This approach
facilitates modular and concurrent operations, enhancing the system's
scalability and responsiveness. The [report](report.pdf) further explores our
architectural choices and provides insights into how the implementation achieves
the project's objectives of consistency and reliability within the context of
modern distributed systems.


## Setup & Usage

Before you begin, ensure that you have all required dependencies from the [Akka
framework](https://akka.io/) and an up-to-date Java version installed.

With the dependencies in place, populate the `commands.txt` file with the
desired commands for execution. To run the project, navigate to this directory
and execute:
```
gradle run
```

If set up correctly, the system will process the commands from `commands.txt`.
Shortly after, a command prompt will appear, ready to receive additional
instructions.

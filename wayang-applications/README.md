# Wayang Applications

This module provides some example applications for using Apache Wayang in industrie specific scenarios. 

## Example 1
Our traditional word-count example for Kafka topics is provided in the script:

```bash
run_wordcount_kafka.sh
```

This script needs the configuration files:

```bash
source .env.sh
source env.demo1.sh
```

Furthermore, the cluster properties are stored in the _default.properties_ file in the module with Kafka-Source and Kafka-Sink components.

**TODO:** We will improve this, by making the path to the Kafka client properties configurable soon.


## Prerequisites

The following scripts use Apache Kafka topics as source and sink:

- run_wordcount_kafka.sh

In order to make the demo working, you need a proper cluster setup. 
Over time, we aim on a robust and reusable DEMO environment.
For the beginning, we use a Confluent cloud cluster, and its specific CLI tool to setup and teardown the topics.

Later on, an improved solution will follow.

For now you need the following tools installed, in addition to the Wayang, Spark, Hadoop libraries:

- Confluent CLI tool.
- jq 

In OSX, both can be installed using homebrew. The installation and setup process for other environment are different.
```bash
brew install confluentinc/tap/cli
brew install jq
```

## Configuration

### Application environment
The file named _.env.sh_ is ignored by git, hence it is the place
for your personal configuration including credentials and cluster coordinates.
An example is given in _env_template.sh_.

### DEMO Setup
The file _env.demo1.sh_ contains additional properties, need in a particlar demo application.

In this file we will never see cluster or user specific details, only properties which are specific to the 
particular application are listed here.



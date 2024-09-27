<!--
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
-->

# Wayang Demo Cluster
This module provides a configuration for our Apache Wayang playground.

It contains the following services:
- Confluent Platform for Kafka topics incl. Schema Registry and Controle Center
- 2 PostgreSQL databases
- Apache Spark cluster with one master and 2 workers
- AKHQ

## Confluent Platform
The Docker image `confluentinc/confluent-local:7.6.2` is part of **Confluent's platform** for managing **Apache Kafka** deployments. This image typically contains components for setting up Kafka clusters, including **Kafka brokers, Zookeeper, Confluent Control Center, Kafka Connect, Schema Registry, and more**. It's used for **local development** or testing Kafka clusters in containerized environments. The version 7.6.2 refers to a specific release that includes certain updates and bug fixes for Confluent's stack based on Kafka.

This image is particularly useful for creating development environments, enabling quick Kafka setup with integrated Confluent tools.

## Postgres
```
docker exec -it postgres psql -U postgres
```

## Spark
https://github.com/mvillarrealb/docker-spark-cluster/tree/master

## AKHQ
The image tchiotludo/akhq is used for AKHQ, a web-based user interface that provides management, monitoring, and analysis tools for Apache Kafka. It allows users to easily interact with Kafka clusters, topics, consumer groups, and messages in real-time. This Docker image is typically used to deploy AKHQ in containerized environments.

Key features of AKHQ include:

	•	Viewing and managing topics: Listing Kafka topics, searching messages, creating, deleting, or configuring topics.
	•	Monitoring consumer groups: Checking the status, offsets, and lag for consumer groups.
	•	Viewing Kafka messages: Browsing messages, searching, and filtering based on keys or values.
	•	Schema Registry support: Managing schemas if you’re using Kafka’s schema registry.

This makes it a useful tool for developers, data engineers, and administrators working with Kafka, as it provides a GUI to interact with Kafka more effectively.

You can find more details on the official GitHub repository for AKHQ: https://github.com/tchiotludo/akhq.

## Metabase
The `metabase` service in your Docker Compose setup uses the **Metabase** Docker image (`metabase/metabase:latest`). **Metabase** is an open-source **business intelligence** (BI) tool designed for interactive data analysis and dashboarding.

In this setup:

- The `depends_on` ensures that Metabase waits for the **Postgres** service to be up and running before it starts.
- Metabase will be accessible on **port 3000** of the host machine.
- It's part of the `democluster_net` network, meaning it will communicate with other services in the same network, like Postgres, for querying data.

This configuration allows you to deploy Metabase to query and visualize data from the PostgreSQL database and other supported databases.

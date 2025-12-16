<!--

  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

-->

---
title: Spark Dataset pipelines
description: How to build Wayang jobs that stay on Spark Datasets/DataFrames from source to sink.
---

Wayang’s Spark backend can now run entire pipelines on Spark `Dataset[Row]` (a.k.a. DataFrames). Use this mode when you ingest from lakehouse formats (Parquet/Delta), interoperate with Spark ML stages, or simply prefer schema-aware processing. This guide explains how to opt in.

## When to use Dataset channels

- **Lakehouse storage:** Reading Parquet/Delta directly into datasets avoids repeated schema inference and keeps Spark’s optimized Parquet reader in play.
- **Spark ML:** Our ML operators already convert RDDs into DataFrames internally. Feeding them a dataset channel skips that conversion and preserves column names.
- **Federated pipelines:** You can mix dataset-backed stages on Spark with other platforms; Wayang will insert conversions only when strictly necessary.

## Enable Dataset sources and sinks

1. **Plan builder APIs:**
   - `PlanBuilder.readParquetAsDataset(...)` (Scala/Java) loads Parquet files into a `DatasetChannel` instead of an `RddChannel`.
   - `DataQuanta.writeParquetAsDataset(...)` writes a dataset back to Parquet without converting to RDD first.
2. **Prefer dataset-friendly operators:** Most unary/binary operators accept either channel type, but custom operators can advertise dataset descriptors explicitly. See `DatasetChannel` in `wayang-platforms/wayang-spark` for details.
3. **Let the optimizer keep it:** The optimizer now assigns costs to Dataset↔RDD conversions, so once your plan starts with a dataset channel it will stay in dataset form unless an operator demands an RDD.

## Mixing with RDD operators

If a stage only supports RDDs, Wayang inserts conversion operators automatically:

- `SparkRddToDatasetOperator` converts an RDD of `org.apache.wayang.basic.data.Record` into a Spark `Dataset[Row]` (using sampled schema inference or `RecordType`).
- `SparkDatasetToRddOperator` turns a `Dataset[Row]` back into a JavaRDD.`Record`.

Both conversions carry non-trivial load profiles. You’ll see them in plan explanations if you mix dataset- and RDD-only operators.

## Developer checklist

- **Use `RecordType` when possible.** Providing field names in your logical operators helps the converter derive a precise schema.
- **Re-use `sparkExecutor.ss`.** When writing custom Spark operators that build DataFrames, use the provided `SparkExecutor` instead of `SparkSession.builder()` to avoid extra contexts.
- **Watch plan explanations.** Run `PlanBuilder.buildAndExplain(true)` to verify whether conversions are inserted. If they are, consider adding dataset descriptors to your operators.

## Current limitations

- Only Parquet sources/sinks expose dataset-specific APIs today. Text/Object sources still produce RDD channels.
- ML4All pipelines currently emit plain `double[]`/`Double` RDDs. They still benefit from the internal DataFrame conversions but do not expose dataset channels yet.

Contributions to widen dataset support (e.g., dataset-aware `map`/`filter` or ML4All stages) are welcome.

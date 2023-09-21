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
## EdgeDataQuantaBuilder Trait
- A trait that extends `DataQuantaBuilder` to offer graph-specific operations.

### Key Methods:
1. **pageRank(numIterations: Int)**
   - Feeds built `DataQuanta` into a `PageRankOperator`.
   - Parameters: `numIterations` for the PageRank algorithm.
   - Return: An instance representing the `PageRankOperator`'s output.

## EdgeDataQuantaBuilderDecorator Class
- Acts as a decorator to enrich a regular `DataQuantaBuilder` with operations of a `RecordDataQuantaBuilder`.

### Properties:
- **baseBuilder**: The base `DataQuantaBuilder` that will be enriched.

## PageRankDataQuantaBuilder Class
- An implementation of `DataQuantaBuilder` for `MapOperator` objects, focused on handling the PageRank algorithm.

### Properties:
- **graphDensity**: Represents the presumed graph density.
- **dampingFactor**: Damping factor for the PageRank algorithm.

### Key Methods:
1. **withDampingFactor(dampingFactor: Double)**
   - Sets the damping factor for the PageRank algorithm.
2. **withGraphDensity(graphDensity: ProbabilisticDoubleInterval)**
   - Sets the graph density for the PageRank algorithm.

### Observations:
- The file appears to be centered around handling the PageRank algorithm within the context of data quanta in a graph.
- The builder pattern is utilized to facilitate the creation and configuration of `EdgeDataQuanta` objects.
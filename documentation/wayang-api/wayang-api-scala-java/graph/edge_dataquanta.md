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

## EdgeDataQuanta Class
- `EdgeDataQuanta` enhances the functionality of `DataQuanta` with `Record` objects, representing data flows as edges in a computational graph.

### Comments:
- The class enhances `DataQuanta` functionality with `Record` objects.

### Key Properties:
- **planBuilder**: Implicit property that derives a plan builder from `dataQuanta`.

### Key Methods:
1. **pageRank(numIterations: Int, graphDensity: ProbabilisticDoubleInterval)**
   - Feeds this instance into a `PageRankOperator`.
   - Parameters: `numIterations` (default 20), `graphDensity` (default is `PageRankOperator.DEFAULT_GRAPH_DENSITIY`).
   - Return: A new `DataQuanta` instance representing the `PageRankOperator` output.

### Observations:
- The class focuses on graph edge functionalities, particularly related to the PageRank algorithm.
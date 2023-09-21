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

### Documentation Comments:
- A TODO comment mentions adding unitary tests to the elements in this file.
- Represents an intermediate result/data flow edge in a `WayangPlan`.

### Class Definition: DataQuanta[Out: ClassTag]
- Represents intermediate data or flow edges in a `WayangPlan`.
- Takes an operator, an output index, and an implicit plan builder as parameters.

#### Key Methods:
1. `output`: Returns the corresponding `OutputSlot` of a wrapped `Operator`.
2. `map`: Feeds the instance into a `MapOperator`.
3. `mapJava`: Similar to `map`, but for Java 8 lambdas.
4. `project`: Feeds the instance into a `MapOperator` with a `ProjectionDescriptor`.
5. `connectTo`: Connects the `operator` to another `Operator`.
6. `filter`: Feeds the instance into a `FilterOperator`.
7. `filterJava`: Similar to `filter`, but for Java 8 lambdas.
8. `flatMap`: Appears to feed the instance into a `FlatMapOperator`.
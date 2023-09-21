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

## JoinedDataQuanta[Out0: ClassTag, Out1: ClassTag]
- Represents a class that is the result of a join operation between two data quanta.
- Contains an embedded `dataQuanta` representing the result of the join operation.

### Key Methods:
1. **assemble[NewOut: ClassTag]**
   - Assembles a new element from a join product tuple.
   - Parameters: `udf` (transformation function), `udfLoad` (load profile estimator).
   - Returns: The join product `DataQuanta`.

2. **assembleJava[NewOut: ClassTag]**
   - Assembles a new element from a join product tuple using a Java function.
   - Parameters: `assembler` (Java transformation function), `udfLoad` (load profile estimator).
   - Returns: The join product `DataQuanta`.
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

## KeyedDataQuanta[Out: ClassTag, Key: ClassTag]
- Represents a class that provides operations on data with functionalities for keyed data.
- Contains an embedded key extractor of type `SerializableFunction[Out, Key]`.

### Key Methods:
1. **join[ThatOut: ClassTag]**
   - Performs a join using the keys of `KeyedDataQuanta`.
   - Parameters: `that`, the other `KeyedDataQuanta` to join with.
   - Returns: The join product `DataQuanta`.

2. **coGroup[ThatOut: ClassTag]**
   - Performs a co-group using the keys of `KeyedDataQuanta`.
   - Parameters: `that`, the other `KeyedDataQuanta` to co-group with.
   - Returns: The co-grouped `DataQuanta`.

### Additional Class:
- **JoinedDataQuanta[Out0: ClassTag, Out1: ClassTag]**: Likely represents the output of join operations.
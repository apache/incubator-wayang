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
The `MapFunctionImpl` inner class resides within the `WayangProjectVisitor` class in the `converter` sub-package of the `calcite` package in the `wayang-api-sql` module. This class serves as the mapping function for the `MapOperator` used in SQL projections.

## Class Functionality:

1. **Implementation**: 
   - Implements the `FunctionDescriptor.SerializableFunction` interface.
   - Allows the class to be serialized and used within Wayang's distributed processing model.

2. **Fields**:
   - Contains a field named `fields`.
   - Holds the indices of the projected fields.

3. **Constructor**:
   - Initializes the `fields` based on the provided projects.

4. **Utility Method - `getProjectFields`**:
   - A static method.
   - Extracts the field indices from a list of `RexNode` projects.

5. **Mapping Functionality - `apply` Method**:
   - Takes a `Record` as input.
   - Constructs a new `Record` based on the projected fields.
   - Retrieves values from the input record and sets them in the new record based on the specified projections.

This class is pivotal in the projection process, ensuring that the specified fields in a SQL projection are retained in the output while all other fields are discarded.
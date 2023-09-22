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
This documentation provides an in-depth understanding of the `apply` method within the `MapFunctionImpl` inner class of the `WayangProjectVisitor` class.

## Method Functionality:


The `apply` method of the `MapFunctionImpl` inner class is pivotal in the projection process. Its primary function is to transform an input `Record` based on the specified projection fields. Here's a detailed understanding:

1. **Method Signature**: `Record apply(Tuple2<Record, Record> record)`
   - **Input**: A `Tuple2<Record, Record>` object named `record`.
   - **Output**: A transformed `Record` object that contains only the projected fields.

2. **Creating a New Record for Projection**:
   - The method initiates by creating a new `Record` object with a size equal to the number of projected fields. This new record will store the values of the projected fields from the input record.

3. **Populating the New Record**:
   - The method iterates over the `fields` array, which contains the indices of the projected fields.
   - For each index in the `fields` array, it retrieves the corresponding value from the input record and sets it in the new record at the respective position.
   - This operation effectively filters out any field that is not part of the projection.

4. **Returning the Projected Record**:
   - After populating the new record with the projected fields, the method returns this newly constructed record.

This method plays a vital role in ensuring that only the specified fields in a SQL projection are retained in the output. All other fields from the input record are discarded.
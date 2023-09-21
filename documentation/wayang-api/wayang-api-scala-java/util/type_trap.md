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

## TypeTrap
- `TypeTrap` is a utility class designed to handle type-related functionalities within Wayang.

### Comments:
- The class awaits a `DataSetType` to be set and ensures there are no two different sets.

### Key Properties:
- **_dataSetType**: Private property that stores the `DataSetType` instance.

### Key Methods:
1. **dataSetType_=(dst: DataSetType[_])**
   - Sets the `DataSetType` for this instance.
   - Parameters: `dst`.
   - Throws: `IllegalArgumentException` if a different `DataSetType` was set previously.

2. **dataSetType**
   - Retrieves the previously set `DataSetType`.
   - Return: Previous `DataSetType` or `DataSetType.none()` if none was set.

3. **typeClass**
   - Returns the `Class` of the previously set `DataSetType`.
   - Return: `Class` of the previous `DataSetType` or `DataSetType.none()` if none was set.

### Observations:
- The utility ensures type consistency and will raise an exception if a conflicting type is set.
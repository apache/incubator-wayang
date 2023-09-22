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
### Class Definition
The `PythonFunctionWrapper` class serves as a bridge between the Java and Python UDFs in the Wayang framework. It ensures that Python functions can be seamlessly integrated into the Java environment of Wayang.

### Attributes:
1. **myUDF (PythonUDF<Input, Output>)**: Represents the Python user-defined function.
2. **serializedUDF (PythonCode)**: Serialized form of the Python UDF.

### Constructor:
- `PythonFunctionWrapper(PythonUDF<Input, Output> myUDF, PythonCode serializedUDF)`:
  - Initializes the wrapper with a Python UDF and its serialized form.

### Methods:
1. **apply(Iterable<Input> input)**:
   - Executes the Python UDF on the input data and returns the results.

---


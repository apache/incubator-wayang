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
## function

### PythonFunctionWrapper Class
- Acts as a Java wrapper for Python functions.
- Implements `FunctionDescriptor.SerializableFunction<Iterable<Input>, Iterable<Output>>`.
- Attributes:
  - `PythonUDF<Input, Output> myUDF`: Stores the Python UDF the wrapper class handles.
  - `PythonCode serializedUDF`: Stores a serialized version of the Python UDF.

### PythonUDF Interface
- Represents Python functions to be used with the Wayang framework.
- Extends `FunctionDescriptor.SerializableFunction<Iterable<Input>, Iterable<Output>>`.
- Does not define any additional methods.

### PythonCode Class
- Represents serialized Python code snippets or blocks.
- Implements the `Serializable` interface for efficient data storage or transmission.
- Attribute `byte[] code`: Stores the serialized Python code.

---

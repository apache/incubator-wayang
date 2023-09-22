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
### Interface Definition
The `PythonUDF<Input, Output>` interface represents Python user-defined functions (UDFs) within the Wayang framework. It is designed to represent functions that handle iterable input and produce iterable output, ensuring seamless integration of Python functions with the Wayang Java framework.

### Inherits:
- `FunctionDescriptor.SerializableFunction<Iterable<Input>, Iterable<Output>>`: Represents serializable functions that operate on iterables.

### Key Points:
- The `PythonUDF` interface enforces a specific function signature on the classes that implement it.
- It plays a vital role in the bridge between Wayang's Java framework and Python UDFs.

---


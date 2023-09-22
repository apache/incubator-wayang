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
## Executor - wayang-api-python

### PythonWorkerManager Class
- Manages the execution of Python UDFs within the Wayang framework.
- Handles Python worker subprocesses, feeding them input data, and potentially receiving results.

### PythonProcessCaller Class
- Manages the execution of Python code within a separate process.
- Sets up socket communication for interaction with the Python process and provides methods to manage the process lifecycle.

### ProcessFeeder Class
- Feeds input data to a Python process for execution.
- Ensures proper serialization of data and manages socket communication for data transmission.

### ProcessReceiver Class
- Handles receipt of data from the Python process.
- Reads data over a socket connection and provides an iterator for accessing the received data.

### ReaderIterator Class
- An iterator over a data stream, specifically designed to read serialized data objects.
- Facilitates reading data sent from the Python process to the Java environment.

---


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
The `PythonProcessCaller` class manages the execution of Python code within a separate process in the Wayang framework. It sets up socket communication for interaction with the Python process and provides methods to manage the process lifecycle.

### Attributes:
1. **process (Thread)**: Represents the Python process or worker being managed.
2. **socket (Socket)**: Socket for communication with the Python process.
3. **serverSocket (ServerSocket)**: Server socket to listen for connections from the Python process.
4. **ready (boolean)**: Indicates the readiness state of the Python process.
5. **configuration (Configuration)**: Configuration settings, potentially related to executing Python tasks.

### Constructor:
- `PythonProcessCaller(PythonCode serializedUDF)`:
  - Initializes the process caller with a serialized Python UDF.
  - Sets up a configuration, loads it from a properties file (`wayang-api-python-defaults.properties`).
  - Initializes a server socket bound to a local address (127.0.0.1) and a dynamic port.
  - Launches a separate thread to start a Python worker process, with the required environment set up, and expects it to connect back via the server socket.

---


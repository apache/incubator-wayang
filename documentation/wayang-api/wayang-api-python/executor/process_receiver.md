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
The `ProcessReceiver` class is responsible for receiving data from a Python process within the Wayang framework. It manages socket communication for data reception and provides methods to retrieve and print the received data.

### Attributes:
1. **iterator (ReaderIterator<Output>)**: Iterator over the data being read from the Python process.

### Constructor:
- `ProcessReceiver(Socket socket)`:
  - Sets up a data input stream attached to the socket's input stream and initializes the `iterator` attribute.

### Methods:
1. **getIterable()**:
   - Returns an iterable that uses the `iterator` attribute.
  
2. **print()**:
   - Prints the received data to the console.

---


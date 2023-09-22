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
The `ReaderIterator` class provides iterator-based access to objects read from a data input stream. It efficiently processes data received from the Python process in a streaming manner.

### Attributes:
1. **nextObj (Output)**: The next object to be returned by the iterator.
2. **eos (boolean)**: Flag indicating the end of the data stream.
3. **fst (boolean)**: Unclear from the provided content; possibly a flag for some state.
4. **stream (DataInputStream)**: The input stream from which data is read.

### Constructor:
- `ReaderIterator(DataInputStream stream)`:
  - Initializes the iterator with the data input stream.

### Methods:
1. **read()**:
   - Reads and decodes an object from the stream.
   - Handles end of stream and relevant exceptions.

2. **hasNext()**:
   - Checks if there's another object to read.

3. **next()**:
   - Returns the next object or throws an exception if there isn't one.

---


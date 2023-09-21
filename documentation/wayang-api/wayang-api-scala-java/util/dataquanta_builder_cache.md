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

## DataQuantaBuilderCache
- This class caches products of `DataQuantaBuilder` that need to be executed simultaneously.

### Key Properties:
- **_cache**: An `IndexedSeq` of `DataQuanta[_]` representing the cache storage.

### Key Methods:
1. **hasCached**
   - Determines if there's content in the cache.
   - Returns: Boolean indicating cache status.

2. **apply[T](index: Int)**
   - Retrieves cached `DataQuanta` by index.
   - Parameters: `index`.
   - Return: Cached `DataQuanta` of type `T`.

3. **cache(dataQuanta: Iterable[DataQuanta[_]])**
   - Caches `DataQuanta`. Should be called once.
   - Parameters: `dataQuanta`.

### Observations:
- The class provides caching utilities to optimize certain operations.
- It offers methods to check, retrieve, and populate the cache.
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

## util Module

### DataQuantaBuilderDecorator.scala
- Provides a utility to extend a `DataQuantaBuilder`'s functionality by decoration.
- Contains foundational methods providing core functionalities and configurations for the `DataQuantaBuilder`.

### DataQuantaBuilderCache.scala
- Manages caching for `DataQuantaBuilder` products.
- Offers caching functionalities for `DataQuantaBuilder` to ensure efficient and consistent data handling.

### TypeTrap.scala
- Manages and verifies types for datasets.
- Designed to ensure consistent type information for datasets.

## graph Module

### Edge.scala
- Contains utility methods related to edge creation and management.
- Provides utilities for creating and handling edges, fundamental entities in graph structures.

### EdgeDataQuanta.scala
- Enhances `DataQuanta` with functionalities specific to edges.
- Extends the capabilities of `DataQuanta` to handle data flows represented as edges.

### EdgeDataQuantaBuilder.scala
- Enriches the `DataQuantaBuilder` with graph-specific operations tailored for edges.
- Provides functionalities to build and handle data flows represented as edges.
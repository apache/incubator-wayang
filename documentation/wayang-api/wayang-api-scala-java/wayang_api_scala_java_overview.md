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
# wayang-api-scala-java Overview

## DataQuanta.scala

- **DataQuanta[Out: ClassTag]**: Represents an intermediate result or data flow edge in a WayangPlan.
- **KeyedDataQuanta[Out: ClassTag, Key: ClassTag]**: Provides operations on DataQuanta with additional operations for keyed data.
- **JoinedDataQuanta[Out0: ClassTag, Out1: ClassTag]**: Represents the result of a join operation between two data quanta.
- **DataQuanta Object**: Provides utility methods or factories related to DataQuanta.

## util Directory

- **DataQuantaBuilderDecorator.scala**: Defines a decorator pattern for DataQuantaBuilder.
- **DataQuantaBuilderCache.scala**: Defines caching functionalities for the DataQuantaBuilder.
- **TypeTrap.scala**: Involved in type-related functionalities or utilities.

## graph Directory

- **Edge.scala**: Defines the concept of an edge in a graph representation.
- **EdgeDataQuanta.scala**: Represents data flows as edges in a computational graph.
- **EdgeDataQuantaBuilder.scala**: Builder pattern for creating EdgeDataQuanta objects.

---

This document provides a high-level overview of the wayang-api-scala-java sub-module. For detailed implementation and interactions, refer to the source code.

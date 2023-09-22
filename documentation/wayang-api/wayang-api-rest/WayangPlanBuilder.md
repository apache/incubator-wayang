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
## Overview

The `WayangPlanBuilder` class is responsible for building or decoding Wayang plans. It likely serves as an intermediary to convert serialized Wayang plans into Wayang's internal representation and vice-versa.

## Attributes

- **`wayangPlan`**: Private attribute of type `WayangPlan`. Represents the primary Wayang plan being managed by the class.
- **`wayangContext`**: Private attribute of type `WayangContext`. Holds essential configurations and settings for executing the Wayang plan.

## Constructor

```java
public WayangPlanBuilder(FileInputStream planFile) {
    // Constructor details ...
}
```

This constructor accepts a `FileInputStream` representing a serialized Wayang plan. It parses the plan and sets up the necessary execution context and plan instance.

## Methods

### buildContext

```java
Method details follow.
```

### buildPlan

```java
Method details follow.
```

The `buildContext` and `buildPlan` methods are invoked by the constructor and are essential for setting up the Wayang context and plan, respectively.

## Summary

The `WayangPlanBuilder` class plays a pivotal role in managing Wayang plans, especially when they are received or sent in a serialized format, potentially over the REST API.


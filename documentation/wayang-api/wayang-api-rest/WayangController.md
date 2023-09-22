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

The `WayangController` class is a central controller for the RESTful API of the Wayang platform. It is responsible for handling HTTP requests related to Wayang plans and their executions.

## Method: `planFromFile`

**Endpoint**: `@GetMapping("/plan/create/fromfile")`
- This annotation suggests that this method handles GET requests to the `/plan/create/fromfile` endpoint.

**Functionality**:
- The method reads a file named `wayang_message` from the `protobuf` directory.
- It then initializes a `WayangPlanBuilder` with the content of this file, which presumably contains a serialized Wayang plan.
- The Wayang plan is executed using the `WayangContext` obtained from the `WayangPlanBuilder`.

This method seems to be responsible for executing Wayang plans provided as files. However, the direct file usage (instead of a passed file parameter) suggests that it might be in a developmental or testing phase.

## Other Methods

### @GetMapping

- This method handles HTTP requests.


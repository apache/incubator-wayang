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

## DataQuantaBuilderDecorator
- This utility extends a `DataQuantaBuilder`'s functionality through decoration.

### Comments:
- There's a note indicating the need to add documentation to the methods of `org.apache.wayang.api.util.DataQuantaBuilderDecorator`.

### Key Methods:
1. **outputTypeTrap**
   - Represents the type of the `DataQuanta` to be built.
   - Return: `TypeTrap` of the base builder's output type.

2. **javaPlanBuilder**
   - Provides a `JavaPlanBuilder` to which this instance is associated.
   - Return: `JavaPlanBuilder` of the base builder.

3. **withName**
   - Sets a name for the `DataQuanta`.
   - Parameters: `name`.
   - Return: Current instance with updated name.

4. **withExperiment**
   - Associates an experiment with the `DataQuanta`.
   - Parameters: `experiment`.
   - Return: Current instance with associated experiment.

... (And several other methods for setting or updating properties of the `DataQuantaBuilderDecorator`).

### Observations:
- Most methods in this utility are fluent setters, allowing for method chaining.
- The utility acts as a wrapper around another `DataQuantaBuilder`, referred to as `baseBuilder`.
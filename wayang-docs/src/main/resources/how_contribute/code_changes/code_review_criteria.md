---
license: |
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
layout: default
title: "Code Review Criteria"
previous:
    url: /how_contribute/code_changes/choosing_what_contribute/
    title: Choosing What to Contribute
next:
    url: /how_contribute/code_changes/preparing_contribute_code_changes/
    title: Preparing to Contribute Code Changes
menus:
    code_changes:
        weight: 2
---
# Code Review Criteria

Before considering how to contribute code, it’s useful to understand how code is reviewed, and why changes may be rejected. See the [detailed guide for code reviewers](https://google.github.io/eng-practices/review/) from Google’s Engineering Practices documentation. Simply put, changes that have many or large positives, and few negative effects or risks, are much more likely to be merged, and merged quickly. Risky and less valuable changes are very unlikely to be merged, and may be rejected outright rather than receive iterations of review.

**Positives**

* Fixes the root cause of a bug in existing functionality
* Adds functionality or fixes a problem needed by a large number of users
* Simple, targeted
* Maintains or improves consistency across Python, Java, Scala
* Easily tested; has tests
* Reduces complexity and lines of code
* Change has already been discussed and is known to committers
* Contribute in the test coverage of the code

**Negatives, Risks**

* Band-aids a symptom of a bug only
* Introduces complex new functionality, especially an API that needs to be supported
* Adds complexity that only helps a niche use case, may be can generate a library then be part of the core.
* Changes a public API or semantics \(rarely allowed\)
* Adds large dependencies
* Changes versions of existing dependencies, if is not mandatory or for security reasons.
* Adds a large amount of code
* Makes lots of modifications in one “big bang” change, but can be considere in a new version.


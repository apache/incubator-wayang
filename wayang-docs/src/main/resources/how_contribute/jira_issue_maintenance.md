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
title: "Jira Issue Maintenance"
previous:
    url: /how_contribute/bugs_report/
    title: Bugs Report
next:
    url: /how_contribute/documentation_changes/
    title: Documentation Changes
menus:
    contribute:
        weight: 2
---
# Jira Issue Maintenance

Given the sheer volume of issues raised in the [Jira Issues](https://issues.apache.org/jira/projects/WAYANG/issues), inevitably some issues are duplicates, or become obsolete and eventually fixed otherwise, or can’t be reproduced, or could benefit from more detail, and so on. It’s useful to help identify these issues and resolve them, either by advancing the discussion or even resolving the [Issues](https://issues.apache.org/jira/projects/WAYANG/issues). Most contributors are able to directly resolve [Issues](https://issues.apache.org/jira/projects/WAYANG/issues). Use judgment in determining whether you are quite confident the issue should be resolved, although changes can be easily undone. If in doubt, just leave a comment on the [Issues](https://issues.apache.org/jira/projects/WAYANG/issues).

When resolving [Issue](https://issues.apache.org/jira/projects/WAYANG/issues), observe a few useful conventions:

* Resolve as **Fixed** if there’s a change you can point to that resolved the issue
    * Set Fix Version\(s\), if and only if the resolution is Fixed
    * Set Assignee to the person who most contributed to the resolution, which is usually the person who opened the PR that resolved the issue.
    * In case several people contributed, prefer to assign to the more ‘junior’, non-committer contributor
* For issues that can’t be reproduced against master as reported, resolve as **Cannot Reproduce**
    * Fixed is reasonable too, if it’s clear what other previous pull request resolved it. Link to it.
* If the issue is the same as or a subset of another issue, resolved as **Duplicate**
    * Make sure to link to the [Issue](https://issues.apache.org/jira/projects/WAYANG/issues) it duplicates
    * Prefer to resolve the issue that has less activity or discussion as the duplicate
* If the issue seems clearly obsolete and applies to issues or components that have changed radically since it was opened, resolve as **Not a Problem**
* If the issue doesn’t make sense – not actionable, for example, a non-Wayang issue, resolve as **Invalid,** but if produce by an underline platform please link the report on the underline platform
* If it’s a coherent issue, but there is a clear indication that there is not support or interest in acting on it, then resolve as **Won’t Fix,** and explain the "why" we will not fix it**.**
* Umbrellas are frequently marked **Done** if they are just container issues that don’t correspond to an actionable change of their own

## Types of Label of the Issues


* Change : Requesting a change in the current IT profile.

* IT help: Requesting help for an IT related problem.

* Incident: Reporting an incident or IT service outage.

* New feature: Requesting new capability or software feature.

* Problem: Investigating and reporting the root cause of multiple incidents.

* Service request: Requesting help from an internal or customer service team.

* Service request with approval: Requesting help that requires a manager or board approval.

* Support: Requesting help for customer support issues. 

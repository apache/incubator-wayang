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
title: "Bugs Report"
previous:
    url: /how_contribute/
    title: How to Contribute
next:
    url: /how_contribute/jira_issue_maintenance/
    title: Jira Issue Maintenance
menus:
    contribute:
        weight: 1
---

# Bug Reports

## How to make a good Bug Report?

Ideally, bug reports are accompanied by a proposed code change to fix the bug. This isn’t always possible, as those who **discover a bug may not have the experience to fix it, however, it needs to be reported**  by creating a [Jira Issues](https://issues.apache.org/jira/projects/WAYANG/issues).

Before to contiues reading take a look on how to [Create Issues](https://support.atlassian.com/jira-software-cloud/docs/create-an-issue-and-a-sub-task/)

Have in mind that a bug reports are only useful when they include enough information to understand, isolate and ideally reproduce the bug. Simply encountering an error does not mean a bug should be reported; as below, search in [Jira Issues](https://issues.apache.org/jira/projects/WAYANG/issues). Unreproducible bugs, or simple error reports, may be closed if is not possible to collect enough information.

It’s very helpful if the bug report has a description about how the bug was found it, the version of Wayang where occurs and the platform that was be setted at that moment, so that reviewers can easily understand the bug. It also helps committers to narrow down the problem to the root cause, because **it can be bug of the underline platform,** but is important figure out.

**Performance regression** is also one kind of bug. The pull request to fix a performance regression must provide a benchmark to prove the problem is indeed fixed.

Note that, **data correctness/data loss bugs are very serious**. Make sure of labeling the bug as `correctness` or `data-loss`, when you create the [Jira Issue](https://issues.apache.org/jira/projects/WAYANG/issues). If the bug report doesn’t get enough attention, please send an email to `dev@wayang.apache.org`, to draw more attentions.

It is possible to propose new features as well. These are generally not helpful unless accompanied by detail, such as a design document and/or code change. Large new contributions should consider to be part of `wayang-core` but first needs to be discussed on the [Slack Channel](https://the-asf.slack.com/archives/C01H1CPE8KU). Feature requests may be rejected, or closed after a long period of inactivity.

### Label meaning

Description of error types:

* Functional error: It is a Generic error type that requires further analysis. Happens whenever software does not behave as intended. For example, if the end user clicks the “Save” button, but their entered data isn’t saved, this is a functional error.
* Logic error: The error represents a mistake in the software flow and causes the software to behave incorrectly. This type of error can cause the program to produce an incorrect output, or even hang or crash.
* Calculation error: Anytime software returns an incorrect value. There may be used an incorrect algorithm or a data type mismatch during arithmetic operations.
* Out of bounds error: End user interacts with the software in ways that were not expected. This often occurs when the user sets a parameter outside the limits of intended use.

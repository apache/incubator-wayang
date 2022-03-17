<!-- markdown-link-check-disable -->

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
title: "Jira issues"
previous:
    url: /how_contribute/code_changes/preparing_contribute_code_changes/
    title: Preparing to Contribute Code Changes
next:
    url: /how_contribute/code_changes/preparing_contribute_code_changes/pull_request/
    title: Pull Request
menus:
    preparing_code_changes:
        weight: 1
---

# Jira issues

Currently, Wayang uses [Jira Issues](https://issues.apache.org/jira/projects/WAYANG/issues) to track logical issues, including bugs and improvements, and uses GitHub pull requests to manage the review and merge of specific code changes. That is, [Jira Issues](https://issues.apache.org/jira/projects/WAYANG/issues) are used to describe _what_ should be fixed or changed, and high-level approaches, and pull requests describe _how_ to implement that change in the project’s source code. For example, major design decisions are discussed in [Jira Issues](https://issues.apache.org/jira/projects/WAYANG/issues).

1. Find the existing [Jira Issues](https://issues.apache.org/jira/projects/WAYANG/issues) that the change pertains to.
    1. Do not create a new [Jira Issue](https://issues.apache.org/jira/projects/WAYANG/issues) if creating a change to address an existing issue in [Jira Issues](https://issues.apache.org/jira/projects/WAYANG/issues); add to the existing discussion and work instead
    2. Look for existing pull requests that are linked from the [Jira Issues](https://issues.apache.org/jira/projects/WAYANG/issues), to understand if someone is already working on the [Jira Issues](https://issues.apache.org/jira/projects/WAYANG/issues).
2. If the change is new, then it usually needs a new [Jira Issues](https://issues.apache.org/jira/projects/WAYANG/issues). However, trivial changes, where the what should change is virtually the same as the how it should change do not require a [Jira Issues](https://issues.apache.org/jira/projects/WAYANG/issues).     Example: `Fix typos in Foo scaladoc`
3. If required, create a new [Jira Issues](https://issues.apache.org/jira/projects/WAYANG/issues):
    1. Provide a descriptive Title. “Update web UI” or “Problem in scheduler” is not sufficient. “Kafka Streaming support fails to handle empty queue in YARN cluster mode” is good.
    2. Write a detailed Description. For bug reports, this should ideally include a short reproduction of the problem. For new features, it may include a design document.
    3. Set required fields:
        1. **Issue Type**. Generally, Bug, Improvement and New Feature are the only types used in Wayang, look more [here]({% link how_contribute/jira_issue_maintenance.md %}).
        2. **Priority**. Set to Major or below; higher priorities are generally reserved for committers to set. The main exception is correctness or data-loss issues, which can be flagged as Blockers.  Their meaning is roughly:
            1. Blocker: pointless to release without this change as the release would be unusable to a large minority of users. Correctness and data loss issues should be considered Blockers for their target versions.
            2. Critical: a large minority of users are missing important functionality without this, and/or a workaround is difficult.
            3. Major: a small minority of users are missing important functionality without this, and there is a workaround
            4. Minor: a niche use case is missing some support, but it does not affect usage or is easily worked around
            5. Trivial: a nice-to-have change but unlikely to be any problem in practice otherwise
        3. **Component**
        4. **Affects Version**. For Bugs, assign at least one version that is known to exhibit the problem or need the change
        5. **Label**. Not widely used, except for the following:
            * `correctness`: a correctness issue
            * `data-loss`: a data loss issue
            * `release-notes`: the change’s effects need mention in release notes. The [Jira Issues](https://issues.apache.org/jira/projects/WAYANG/issues) or pull request should include detail suitable for inclusion in release notes – see “Docs Text” below.
            * `starter`: small, simple change suitable for new contributors
        6. **Docs Text**: For issues that require an entry in the release notes, this should contain the information that the release manager should include in Release Notes. This should include a short summary of what behavior is impacted, and detail on what behavior changed. It can be provisionally filled out when the [Jira Issues](https://issues.apache.org/jira/projects/WAYANG/issues) is opened, but will likely need to be updated with final details when the issue is resolved.
    4. Do not set the following fields:
        1. **Fix Version**. This is assigned by committers only when resolved.
        2. **Target Version**. This is assigned by committers to indicate a PR has been accepted for possible fix by the target version.
    5. Do not include a patch file; pull requests are used to propose the actual change.
4. If the change is a large change, consider inviting discussion on the issue at [email list\#dev](mailto:dev@wayang.apache.org) first before proceeding to implement the change.


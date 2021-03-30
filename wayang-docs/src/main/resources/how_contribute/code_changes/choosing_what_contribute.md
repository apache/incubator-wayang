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
title: "Choosing What to Contribute"
previous:
    url: /how_contribute/code_changes/
    title: Code Changes
next:
    url: /how_contribute/code_changes/code_review_criteria/
    title: Code Review Criteria
menus:
    code_changes:
        weight: 1
---

# Choosing What to Contribute

Wayang is a great project but currently we are searching for having more hand on the code. Review can take hours or days of committer time. Everyone benefits if contributors focus on changes that are useful, clear, easy to evaluate, and already pass basic checks.

Sometimes, a contributor will already have a particular new change or bug in mind. If seeking ideas, consult the list of starter tasks in [Jira Issues](https://issues.apache.org/jira/projects/WAYANG/issues), or ask the [email list@dev](mailto:dev@wayang.apache.org).

Before proceeding, contributors should evaluate if the proposed change is likely to be relevant, new and actionable:

* Is it clear that code must change? Proposing a [Jira Issues](https://issues.apache.org/jira/projects/WAYANG/issues) and pull request is appropriate only when a clear problem or change has been identified. If simply having trouble using Wayang, use the [email list@dev](mailto:dev@wayang.apache.org)  first, rather than consider filing a [Jira Issues](https://issues.apache.org/jira/projects/WAYANG/issues) or proposing a change. 
  
* Search on dev@wayang.apache.org for related discussions. Often, the problem has been discussed before, with a resolution that doesn’t require a code change, or recording what kinds of changes will not be accepted as a resolution.
* Search [Jira Issues](https://issues.apache.org/jira/projects/WAYANG/issues) for existing issues.
  
* Type `[search terms]` at the search box. If a logically similar issue already exists, then contribute to the discussion on the existing[Jira Issues](https://issues.apache.org/jira/projects/WAYANG/issues) and pull request first, instead of creating a new one.
* Is the scope of the change matched to the contributor’s level of experience? Anyone is qualified to suggest a typo fix, but refactoring core scheduling logic requires much more understanding of Wayang. Some changes require building up experience first.

<div class="alert alert-info" role="alert">
It’s worth reemphasizing that changes to the core of Wayang, or to highly complex and important modules, are more difficult to make correctly. They will be subjected to more scrutiny, and held to a higher standard of review than changes to less critical code. As result of this, is important to be pacient in the coordination with Wayang Community to be sure that everything can works well after the change.
</div>


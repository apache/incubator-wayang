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
title: "Branching Strategy"
previous:
    url: /how_contribute/code_changes/code_review_criteria/
    title: Code Review Criteria
next:
    url: /how_contribute/code_changes/preparing_contribute_code_changes/jira_issue/
    title: Jira issues
---
# Apache Wayang-Branching Strategy
This page is a summary from an email thread: https://lists.apache.org/thread/v9nhw3ml585456byd6t6cbn3vbhnzkww

## Branches:
__*release (currently not uesd):*__ 

In the future eventually, this branch represents the current stable state of Wayang. It's where users can find the latest and most stable version of the project. The content of this branch is the same as the latest "rel/x.x.x" branch.
For now, we changed our branching strategy a bit to simplify the process and we will not have a branch called "release" for now. Only the rel/x.x.x


__*rel/x.x.x (Release Version Branches):*__

These branches are used to track all versions of Wayang. Older versions are kept here for reference and use, similar to tags. For example, if you need to use an older version of Wayang, you can find it here.


__*cherry-pick (Feature/Fix Selection):*__

This branch is used when selecting specific features and fixes implemented on the "main" branch to be included in the next release. It allows for testing and ensuring compatibility with Apache guidelines before creating the "rel/" branch.


__*main (Development Branch):*__

This branch is where approved pull requests (PRs) are merged, but it may not always be stable. New content is frequently opened and merged here. Developers work on new features or bug fixes by creating new "feature/bugfix" branches from "main."
From other projects you may know the branch named __develop__ - it is the same as __main__ in our case.

## Workflow:
Developers work primarily on the main branch, where they create and merge PRs for new features or bug fixes.
When it's time to make a new release, features and fixes implemented on main are cherry-picked into the cherry-pick branch. Testing and compliance with Apache guidelines are performed.
After successful testing and compliance, the rel/x.x.x branch is created using Maven commands.
Once the rel/x.x.x branch conforms to Apache approvals, its content is merged into the main branch. This provides an easy and intuitive way for users to find the latest stable version of Wayang.


## Default Branch in GitHub:
The default branch in the GitHub repository can be set to main to reflect the most active development branch. This makes it easier for contributors to find the primary development branch.
This branching strategy aims to maintain stability while allowing for continuous development and easy access to older versions when needed. Developers create and merge features on the develop branch, and only selected changes are included in releases after thorough testing.

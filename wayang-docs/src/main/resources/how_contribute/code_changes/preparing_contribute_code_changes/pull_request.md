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
title: "Pull Request"
previous:
    url: /how_contribute/code_changes/preparing_contribute_code_changes/jira_issue/
    title: Jira issues
next:
    url: /how_contribute/code_changes/preparing_contribute_code_changes/closing_pull_request/
    title: Closing Your Pull Request and Github Issues
menus:
    preparing_code_changes:
        weight: 2
---

# Pull Request

The simplest way to submit code changes, is via a GitHub pull-request.

In order to do this first create a GitHub account and sign into you account.

After that’s done, please visit our GitHub site and create a so-called Fork.



What happens now, is that GitHub creates a full copy of the Wayang repository in your account. Only you can commit to this.

Now ideally you check-out your cloned repository:

* git clone https://github.com/{your-user-id}/incubator-wayang.git

Now you have a copy of Wayang on your computer and you can change whatever you want and as it’s your copy, you can even commit these changes without any danger of breaking things.

As soon as you’re finished with your changes and want us to have a look, it’s time to create a so-called Pull-Request.

You do that by going to your forked repository page on GitHub.

Every forked repository has an additional button called "New Pull Request":

If you click on it, we will receive a notification on your changes and can review them. We also can discuss your changes and have you perfect your pull request before we accept and merge it into Wayang.

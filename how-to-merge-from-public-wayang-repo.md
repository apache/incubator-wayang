<!---
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
--->

## How to merge changes from the public Wayang repository 

### In terminal

- `git remote add incubator-wayang-remote https://github.com/apache/incubator-wayang` This command adds the Wayang repository as a remote. This is a local operation, so you don't need to repeat this step for future merges on the same machine. If you're on a different machine, then you'll need to repeat this step.

- `git checkout -b merge-wayang-0.7.1` Create and switch to a new branch for the merge. Select the branch name appropriately.

- `git fetch incubator-wayang-remote` Fetch the changes from the public repo without modifying your local branches.

- `git merge incubator-wayang-remote/main` Merge the changes from the main branch of the public repo into your current working branch.

The `merge-wayang-0.7.1` branch is now up-to-date with the `incubator-wayang-remote/main` branch. To apply these updates to the `main` branch, do:

- `git checkout main` Switch to the `main` branch.

- `git pull` Pull the latest changes from the remote repository for the `main` branch. Always do this before starting a merge to reduce potential conflicts.

- `git merge merge-wayang-0.7.1` Merge the changes from `merge-wayang-0.7.1` into `main`.

Don't forget to push your changes with `git push origin main` after you're done.


### In IntelliJ IDEA

1. Add new remote repository:
    - Open the **Git** -> **Manage Remotes**. There you should see a list of existing remotes, if any.
    - Click on the **"+"** button to add a new remote. In the opened dialog:
        - For **Name** use "incubator-wayang-remote"
        - For **URL** use "https://github.com/apache/incubator-wayang"
        - Click **OK**
    
    This is a local operation, so you don't need to repeat this step for future merges on the same machine. If you're on a different machine, then you'll need to repeat this step.

2. Create a new local branch and check it out:
    - Go to the **Branches** tool window: **Git** -> **Branches**
    - Click on the **"+"** button -> **New Branch** and name it appropriately e.g., `merge-wayang-0.7.1`

3. Fetch the changes from the public repository:
    - Go to **Git** -> **Fetch**
    - From the dropdown list, choose "incubator-wayang-remote"

4. Merge changes from the remote into your local branch:
    - While on your target branch go to **Git** -> **Merge** to display the merge dialog.
    - Select "incubator-wayang-remote/main" under "Branch to merge".

Your branch `merge-wayang-0.7.1` is now up to date. To merge into `main` do:

1. Checkout to `main` branch:
    - Go to **Git** -> **Branches**, then click on `main` under "Local Branches".

2. Merge `merge-wayang-0.7.1` into `main`:
    - While on `main` branch go to **Git** -> **Merge**.
    - Select `merge-wayang-0.7.1` under "Branch to merge".
# Apache Wayang-Branching Strategy
This page is a summary from an email thread: https://lists.apache.org/thread/v9nhw3ml585456byd6t6cbn3vbhnzkww

## Branches:
__*release (formerly main):*__ 

This branch represents the current stable state of Wayang. It's where users can find the latest and most stable version of the project. The content of this branch is the same as the latest "rel/x.x.x" branch.


__*rel/x.x.x (Release Version Branches):*__

These branches are used to track all versions of Wayang. Older versions are kept here for reference and use, similar to tags. For example, if you need to use an older version of Wayang, you can find it here.


__*cherry-pick (Feature/Fix Selection):*__

This branch is used when selecting specific features and fixes implemented on the "develop" branch to be included in the next release. It allows for testing and ensuring compatibility with Apache guidelines before creating the "rel/" branch.


__*develop (Development Branch):*__

This branch is where approved pull requests (PRs) are merged, but it may not always be stable. New content is frequently opened and merged here. Developers work on new features or bug fixes by creating new "feature/bugfix" branches from "develop."

## Workflow:
Developers work primarily on the develop branch, where they create and merge PRs for new features or bug fixes.
When it's time to make a new release, features and fixes implemented on develop are cherry-picked into the cherry-pick branch. Testing and compliance with Apache guidelines are performed.
After successful testing and compliance, the rel/x.x.x branch is created using Maven commands.
Once the rel/x.x.x branch conforms to Apache approvals, its content is merged into the release (formerly main) branch. This provides an easy and intuitive way for users to find the latest stable version of Wayang.


## Default Branch in GitHub:
The default branch in the GitHub repository can be set to develop to reflect the most active development branch. This makes it easier for contributors to find the primary development branch.
This branching strategy aims to maintain stability while allowing for continuous development and easy access to older versions when needed. Developers create and merge features on the develop branch, and only selected changes are included in releases after thorough testing.

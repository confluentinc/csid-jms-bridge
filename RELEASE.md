## Release process for JMS Bridge
This document describes the release process for the JMS Bridge.

Note: due to current limitation with Semaphore CI being unable to push GitHub tags, the release process is manual.

### Release Steps
For the purposes of the example steps below, we will assume the current version of the JMS Bridge is `3.1.0-SNAPSHOT`, we are building release with version `3.1.0` and tag `v3.1.0` and next version will be `3.1.1-SNAPSHOT`. Substitute the version numbers with actual values as needed.

- Make a release branch `release_3.1.0` off main (Required due to branch protection - commits straight to main branch are not allowed) 
```
    git checkout main
    git pull
    git checkout -b release_3.1.0 
``` 
- Prepare a release using maven release plugin - you can optionally skip running tests as those will be run on Semaphore when release branch is pushed.
```
    mvn release:prepare -DskipTests -Dspotbugs.skip -Dcheckstyle.skip
```
- Enter prompts accordingly - either just accept suggestions or provide your own values - if you need to bump major version etc.
```
    What is the release version for "JMS Bridge Parent"? (jms-bridge-pom) 3.1.0: : 
    What is the SCM release tag or label for "JMS Bridge Parent"? (jms-bridge-pom) v3.1.0: : 
    What is the new development version for "JMS Bridge Parent"? (jms-bridge-pom) 3.1.1-SNAPSHOT: : 
```
- Push the release branch to GitHub
```
    git push origin HEAD
```
- Create a Pull Request from the release branch to main

- Verify that Semaphore job for the Tag is passing (and re-run if failing due to flakiness) - job will be for the Tag that we just pushed (`v3.1.0` in this example)
```
    https://confluentinc.semaphoreci.com/projects/csid-jms-bridge
```
- IMPORTANT!!! - when merging **DO NOT USE squash merge** - **use Rebase merge** as we want the commit with Tag to be preserved in Main branch commit history. To use Rebase merge - you may need to enable it in "GH Repository Settings" - tick "Allow rebase merging".
- Once the build is done - merge **(with Rebase)** the PR for the release branch to main.
- Once PR is merged - verify that the Tag is present in the main branch commit history.

- Create a release in Github for the version tag that was just built (v3.1.0 in this example) and generate release notes from diff with previous tag.
- Checkout the release tag and create a package locally for upload to releases page:
```
    git fetch -a
    git checkout v3.1.0
    mvn clean package -DskipTests -Dspotbugs.skip -Dcheckstyle.skip
```
- Upload the `jms-bridge-server-3.1.0-package.zip` zip file created in the `jms-bridge-server/target` directory to GitHub releases page for the release.

- Update documentation (Astrodocs) - add new version slug in `astro.config.mjs`, update changelog with the new release version and release notes and regenerate the docs - follow normal PR process for this.

## Updating Partner Id in the Manifest of JMS Bridge Jar for distribution to specific partners
Script for this process is prepared in `.semaphore/scripts` folder - `update-partner-id.sh`. This script will update the `Partner-Id` in the `MANIFEST.MF` file in the JMS Bridge Jar file. The script requires the following parameters: JMS Bridge Zip file and Partner ID (from Salesforce) to set - for example for partner `123456789`, assuming that current folder is root of the repository and zipfile downloaded to tmp subfolder:
```
    .semaphore/scripts/update-partner-id.sh tmp/jms-bridge-server-3.1.0-package.zip 123456789
```
Note that if manifest entry for Partner-Id is already present - it will be replaced with the new one.

### Partner Id Update steps
- Download release .zip from Github pages for the release you want to distribute
- execute the update script with the downloaded zip file and partner id to set
```
    path/to/code/csid-jms-bridge/.semaphore/scripts/update-partner-id.sh path/to/zipfile/jms-bridge-server-3.1.0-package.zip 123456789
```
- Optionally verify that Manifest entry was added / updated by using `print-manifest-id.sh` script in the `.semaphore/scripts` folder
```
    .semaphore/scripts/print-manifest-id.sh path/to/zipfile/jms-bridge-server-3.1.0-package.zip
```
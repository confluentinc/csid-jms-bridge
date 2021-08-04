# Developer Setup

## Building

To build the project you must have the Confluent Artifactory repository setup as a maven repo on your system.

See https://confluentinc.atlassian.net/wiki/spaces/Engineering/pages/1085800875/Setting+up+Developer+Environment#SettingupDeveloperEnvironment-Maven%2CArtifactory%2CandGradle

Once the maven repo is setup you can build it using maven the usual way from the root of this git
repo run:
```
mvn clean package
```

Optionally to skip tests:
```
mvn clean Package -DskipTests
```


## Releasing

Use the maven release plugin to perform releases
`mvn release:prepare`
`mvn release:perform`

In order to publish a release to github that includes the release archive
you will need to make sure that your `~/.m2/settings.xml` includes credentials for github.
Here is an example:

```xml
     <server>
         <id>github</id>
         <username>your-user-name</username>
         <privateKey>your-oauth-key</privateKey>
     </server>
```
The publication of that release will occur during the `mvn release:perform`.

## IDE

### Intellij

**FreeBuilder Generated Classes**

To get the freebuilder annotation processor generated classes onto the classpath do the following:

1. Rebuild the project
2. From the project window navigate to `target/generated-sources/annotation`
3. Right click on that folder and select `Mark Directory As` -> `Generated Sources Root`

Any code showing errors due to not finding the generated classes should be resolved at this point.
For generated test code it is exactly the same except the source directory and marking are `test`.

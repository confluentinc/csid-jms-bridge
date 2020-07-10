# Developer Setup

## IDE

### Intellij

**FreeBuilder Generated Classes**

To get the freebuilder annotation processor generated classes onto the classpath do the following:

1. Rebuild the project
2. From the project window navigate to `target/generated-sources/annotation`
3. Right click on that folder and select `Mark Directory As` -> `Generated Sources Root`

Any code showing errors due to not finding the generated classes should be resolved at this point.
For generated test code it is exactly the same except the source directory and marking are `test`.

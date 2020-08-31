# Upgrading the JMS Bridge
The JMS Bridge 
## General Upgrade Procedure
Depending on the version of the JMS Bridge that is being upgraded, steps may differ. Please consult the release notes from each version to ensure caveats do not apply.

To upgrade this of the JMS bridge please follow the following steps:

1. Navigate to the etc folder of the broker instance that's being upgraded
2. Open broker.xml and jms-bridge.properties files. It contains a property which is relevant for the upgrade:
    ```console
    ToDo: Upgrade procedures.
    ```

 In most cases the instance can be upgraded to a newer version simply by changing the value of this property to the location of the new broker home. Please refer to the aforementioned versions document for additional upgrade steps (if required).
---
section: Change Log
title: Version
description: Learn about the change log of the JMS Bridge
---

In this section we will provide the following information for each release:

- A link to the full release notes which includes all issues resolved in the release.
- A brief list of "highlights" when applicable.
- If necessary, specific steps required when upgrading from the previous version.
  - Note: If the upgrade spans multiple versions then the steps from each version need to be followed in order.
  - Note: Follow the general upgrade procedure outlined in the Upgrading the Broker chapter in addition to any version-specific upgrade instructions outlined here.

## v. 1.0

### Release Notes

#### Feature Changelog

| Feature                     | Description                                                              | Jira                                                           |
| --------------------------- | ------------------------------------------------------------------------ | -------------------------------------------------------------- |
| JMS-Bridge HA               | Active/Standby HA support for the JMS Bridge                             | [CSID-337](https://confluentinc.atlassian.net/browse/CSID-337) |
| JMS-Bridge Storage in Kafka | All JMS-Bridge journal data stored in Kafka                              | [CSID-242](https://confluentinc.atlassian.net/browse/CSID-242) |
| Documentation Improvements  | Add documentation about collecting telemetry, identify missing telemetry | [CSID-338](https://confluentinc.atlassian.net/browse/CSID-338) |

#### Caveats

| Feature            | Caveats                                                                                                                                |
| ------------------ | -------------------------------------------------------------------------------------------------------------------------------------- |
| JMS-Bridge HA      | Shared Storage, JDBC, and VM HA methods found in ActiveMQ Artemis will no longer be available nor relevant for the JMS-Bridge Project. |
| JMS-Bridge Storage | Alternative storage schema like JdBC, and shared disk, are no longer applicable in the broker.xml configurations.                      |
| JMS-Bridge Storage | rge Message Support has been depricated.                                                                                               |

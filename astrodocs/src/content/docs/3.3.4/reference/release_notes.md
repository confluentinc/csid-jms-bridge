---
title: Release Notes
slug: 3.3.4/reference/release_notes
---

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## v. 1.0.0

### Release Notes

#### Feature Changelog

| Feature                     | Description                                                              | Jira                                                           |
|-----------------------------|--------------------------------------------------------------------------|----------------------------------------------------------------|
| JMS-Bridge HA               | Active/Standby HA support for the JMS Bridge                             | [CSID-337](https://confluentinc.atlassian.net/browse/CSID-337) |
| JMS-Bridge Storage in Kafka | All JMS-Bridge journal data stored in Kafka                              | [CSID-242](https://confluentinc.atlassian.net/browse/CSID-242) |
| Documentation Improvements  | Add documentation about collecting telemetry, identify missing telemetry | [CSID-338](https://confluentinc.atlassian.net/browse/CSID-338) |

#### Caveats

| Feature            | Caveats                                                                                                                                |
|--------------------|----------------------------------------------------------------------------------------------------------------------------------------|
| JMS-Bridge HA      | Shared Storage, JDBC, and VM HA methods found in ActiveMQ Artemis will no longer be available nor relevant for the JMS-Bridge Project. |
| JMS-Bridge Storage | Alternative storage schema like JdBC, and shared disk, are no longer applicable in the broker.xml configurations.                      |
| JMS-Bridge Storage | rge Message Support has been depricated.                                                                                               |

---
title: Release Notes
description: Change log for the JMS Bridge
slug: 3.3.5/reference/release_notes
---

# Release Notes

All notable changes to this project will be documented in this file.

Note that as this release is now in the Confluent Partner GitHub org, the release notes will start at v3.3.4

***

## v. 3.3.5

### Fixes

* Fix for message loss GH issue #29 by @rkolesnev in https://github.com/confluent-partners/csid-jms-bridge/pull/37
* Fix for queue duplication / renaming on load GH issue #34 by @rkolesnev
  in https://github.com/confluent-partners/csid-jms-bridge/pull/42
* Update default init timeout to avoid timing out on single node restart GH issue #35 by @rkolesnev
  in https://github.com/confluent-partners/csid-jms-bridge/pull/43

### Minor changes

* Merge ha main by @jhollandus in https://github.com/confluent-partners/csid-jms-bridge/pull/25
* chore: adding issue template by @eddyv in https://github.com/confluent-partners/csid-jms-bridge/pull/31
* initial addition of jolokia and hawtio to docker build / run flow by @rkolesnev
  in https://github.com/confluent-partners/csid-jms-bridge/pull/32
* correct import in test by @rkolesnev in https://github.com/confluent-partners/csid-jms-bridge/pull/38
* Added hawtio/artemis console instructions as separate HAWTIO.md file by @rkolesnev
  in https://github.com/confluent-partners/csid-jms-bridge/pull/39
* fix startup script - cant have empty param in java exec line by @rkolesnev
  in https://github.com/confluent-partners/csid-jms-bridge/pull/40
* update scm tag to confluent-partners repo by @rkolesnev
  in https://github.com/confluent-partners/csid-jms-bridge/pull/44

**Full Changelog**: https://github.com/confluent-partners/csid-jms-bridge/compare/v3.3.4...v3.3.5

## v. 3.3.4

### Release Notes - 02 Aug 2024

This release contains several fixes and enhancements. The primary objective was to increase the robustness of HA which
was done by reimplementing the node manager using the kafka consumer protocol.

* Updates to documentation and its formatting
* Fix tombstone exchange failure issue
* Integrated the [openmessaging benchmark](https://github.com/openmessaging/benchmark) performance test
* Reimplementation of HA using the kafka consumer leader election protocol
* Observability examples and setup instructions added
* Initial development of chaos testing suite
  |

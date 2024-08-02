# Release Notes


All notable changes to this project will be documented in this file.

Note that as this release is now in the Confluent Partner GitHub org, the release notes will start at v3.3.4

---

## v. 3.3.4

### Release Notes - 02 Aug 2024

This release contains several fixes and enhancements. The primary objective was to increase the robustness of HA which was done by reimplementing the node manager using the kafka consumer protocol.

* Updates to documentation and its formatting
* Fix tombstone exchange failure issue
* Integrated the [openmessaging benchmark](https://github.com/openmessaging/benchmark) performance test
* Reimplementation of HA using the kafka consumer leader election protocol
* Observability examples and setup instructions added
* Initial development of chaos testing suite
  |
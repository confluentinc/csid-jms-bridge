# Frequently Asked Questions

**Q: How do you use it?**

**A:** Upgrade, start up JMS 2.0 bridge, Configure to point to your Confluent cluster, Configure which topics to expose, and to whom, using regex patterns to match topics; and/or use ACLs to control production to and consumption from topics.

**Q: Where does this run? Is this standalone?** 

**A:** The JMS Bridge runs as a separate server, using Confluent for underlying storage. Typically this runs in a container or on a VM. Consider this to have the same requirements that setting up Kafka Streams or ksqlDB would have. All data is always in Kafka, so you can remove the disk, and then rehydrate from Kafka.

**Q: What code or configuration changes are needed? **

**A:** Existing JMS applications will replace their current JMS implementation jars with the provided OSS-engine-based implementation jars. For configurations, it is necessary to specify which topics are exposed, and to whom.

**Q: What are the performance expectations?** 

**A:** This Bridge was not built with performance as the primary concern; the main factors developed for were:
	*	Availability
	* 	Accuracy
	* 	Compatibility

**Q: For a migration use case: does the JMS Bridge support sync request/reply?**

**A:** For JMS clients it does. Kafka clients do not support this concept. Kafka clients may satisfy waiting JMS clients with a response by setting the appropriate Kafka headers that correspond to required JMS headers.

**Q: How do JMS Bridge producers/consumers work with Kafka Partitions and Partition keys?**

**A:** Administrators of the JMS Bridge create routes between JMS topics and Kafka topics and they can configure any JMS property to be the partition key. By default correlation ID is used if present otherwise message ID.

**Q: Does JMS Bridge support JMS Topic to Queue bridge concept?**

**A:** If this is asking if you can bridge between Bridge nodes then yes it does support it but it is most likely not needed since most data is already distributed via Kafka.

**Q: Does JMS Bridge support JMS Bridge queue to Kafka topic?**

**A:** There is no mapping of a queue to a Kafka topic. You would set up routes between JMS topic and Kafka topics, and you can use any JMS property as a partition key. For configuration, the Bridge is based on the OSS Artemis engine. More details at [Configuration Docs](https://confluent-partners.github.io/csid-jms-bridge/reference/configuration/)

**Q: Does JMS Bridge support Transaction functionality?**

**A:** Yes, the JMS Bridge supports transaction functionality. JMS clients support JTA (Java Transaction API); however, Kafka clients do not support JTA.

**Q: How does the JMS Bridge use Kafka producers, and what happens if a Kafka topic encounters issues (e.g., replicas < min.isr)?**

**A:** The JMS Bridge uses two Kafka producers: one to publish messages to the Write-Ahead Log (WAL) and another managed by the Kafka Streams library to process data from the WAL and propagate it to target topics (e.g., orders, users, contracts).  

If the WAL topic encounters issues, such as replicas falling below `min.isr`, new JMS messages will fail to commit to the bridge. If the issue occurs during stream processing, the Kafka Streams processor will stop, halting data propagation to the target topics.


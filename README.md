# Simple Kafka producer/consumer example (w/o Spring)

## Getting Started

Start local Kafka.
Assuming that you are in you local Kafka directory named `kafka` and running `Windows`:

`bin/windows/zookeeper-server-start.bat kafka/config/zookeeper.properties`

`bin/windows/kafka-server-start.bat kafka/config/server.properties`

`bin/windows/kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 13 --topic my-topic`

`bin/windows/kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic my-topic --from-beginning`

Run `KafkaProducerExample.java`, then run `KafkaConsumerExample.java`



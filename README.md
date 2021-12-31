### Install Kafka

- Install via brew: `brew install kafka`
- Configs are located at: `/usr/local/etc/kafka`

### Start Zookeeper

`zookeeper-server-start.sh usr/local/etc/kafka/zookeeper.properties`

### Start Kafka

`kafka-server-start /usr/local/etc/kafka/server.properties`

### Kafka topics

- Create topic : `kafka-topics --bootstrap-server localhost:9092 --create --topic first_topic --partitions 3 --replication-factor 1`
- Delete topic : `kafka-topics --bootstrap-server localhost:9092 --topic first_topic --delete`
- Describe topic: `kafka-topics --bootstrap-server localhost:9092 --topic first_topic --describe`
- List topics: `kafka-topics --bootstrap-server localhost:9092 --list`

### Kafka Console Producer

- basic: `kafka-console-producer --broker-list localhost:9092 --topic first_topic`
- With Producer Property: `kafka-console-producer --broker-list localhost:9092 --topic first_topic --producer-property acks=all`

### Kafka Console Consumer

- Basic: `kafka-console-consumer --bootstrap-server localhost:9092 --topic first_topic`
- All from begining: `kafka-console-consumer --bootstrap-server localhost:9092 --topic first_topic --from-beginning`
- Groups: `kafka-console-consumer --bootstrap-server localhost:9092 --topic first_topic --group my-first-app`

> With the groups, if you run the same command multiple times then the messages consumed will be split among the groups. If we have 3 partitions, then we can have 3 groups so that messages are consumed in a distributed manner.

### Kafka Consumer Groups

- `kafka-consumer-groups --bootstrap-server localhost:9092 --list`
- `kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group my-first-app`
- Reset Offset to begining : `kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group my-first-app --topic first_topic --reset-offsets --to-earliest`
- Reset Offset by shifting : `kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group my-first-app --topic first_topic --reset-offsets --shift-by -2`

> shift-by - shifts either forward or backward. Shifting back by 2 (-2) will result in 6 messages (2 \* 3 paritions) when consumer is started

```
Consumer group 'my-first-app' has no active members.

GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
my-first-app    first_topic     0          6               6               0               -               -               -
my-first-app    first_topic     1          2               2               0               -               -               -
my-first-app    first_topic     2          10              10              0               -
```

> If you have run a consumer for the group , you will see the consumer-id, host which is super helpful. Lag shows how many messages are pending consumption. 0 means we are upto date.

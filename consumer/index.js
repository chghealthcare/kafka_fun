const config = require('./config');
const Scheduler = require('./lib/Scheduler')
const scheduler = new Scheduler();

var kafka = require("kafka-node");
var client = new kafka.KafkaClient({kafkaHost: config.kafka_server});
var consumer = new kafka.Consumer(client, [{ topic: "example", partition: 0 }], {
  autoCommit: true
});

consumer.on("created", (message) => {
  scheduler.distributeMessagesToQueue(message)
});

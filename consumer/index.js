const config = require('./config');
const Scheduler = require('./scheduler')
const scheduler = new Scheduler();

var kafka = require("kafka-node"),
  Consumer = kafka.Consumer,
  client = new kafka.KafkaClient({kafkaHost: config.kafka_server}),
  consumer = new Consumer(client, [{ topic: "example", partition: 0 }], {
    autoCommit: true
  });



consumer.on("message", function (message) {
  scheduler.queueMessage(message)
  // acknowledge that the message was handled to kafka
});

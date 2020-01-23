const config = require('./config');
var kafka = require("kafka-node"),
  Consumer = kafka.Consumer,
  client = new kafka.KafkaClient({kafkaHost: config.kafka_server}),
  consumer = new Consumer(client, [{ topic: "example", partition: 0 }], {
    autoCommit: true
  });

consumer.on("message", function(message) {
  console.log(message);
  
/** { topic: 'cat', value: 'I have 385 cats', offset: 412, partition: 0, highWaterOffset: 413, key: null } */

});
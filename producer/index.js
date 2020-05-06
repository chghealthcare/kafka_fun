const kafka = require('kafka-node');
const config = require('./config');

try {
  const Producer = kafka.Producer;
  const client = new kafka.KafkaClient({kafkaHost: config.kafka_server});
  const producer = new Producer(client);
  const kafka_topic = 'example';

  let payload = {
    topic: kafka_topic,
    messages: "Hello World"
  };

  producer.on('ready', async function() {
    // console.log("Producer is ready")

    setInterval(() => {
      payload = {
        ...payload,
        messages: payload.messages,
        key: `Key ${Math.floor(Math.random() * 3)}`,
        timestamp: Date.now()
      }

      let push_status = producer.send([ payload ], (err, data) => {
        if (err) {
          console.log('[kafka-producer -> '+kafka_topic+']: broker update failed');
        } else {
          // console.log('[kafka-producer -> '+kafka_topic+']: broker update success');
        }
      });
    }, 3000);
  });

  producer.on('error', function(err) {
    console.log(err);
    console.log('[kafka-producer -> '+kafka_topic+']: connection errored');
    throw err;
  });
}
catch(e) {
  console.log(e);
}

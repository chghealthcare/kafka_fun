const { parentPort } = require('worker_threads');

parentPort.on('message', ({ kafkaMessage, kafkaMessageHandler }) => {
  handler(kafkaMessage)
    .then(result => { 
      parentPort.postMessage(result);
    })
    .catch(error => {
      parentPort.postMessage(error);
     })
});
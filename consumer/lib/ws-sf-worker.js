const { parentPort } = require('worker_threads');

const handlers = {
  create: async (message) => {
    console.log("create", message)
  },
  update: async (message) => {
    console.log("update", message)
  },
  delete: async (message) => {
    console.log("delete", message)
  }
}

parentPort.on('message', ({ type, message }) => {
  handlers[type](message)
    .then(result => { 
      parentPort.postMessage(result);
    })
    .catch(error => {
      parentPort.postMessage(error);
    })
});
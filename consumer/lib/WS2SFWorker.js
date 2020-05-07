
const {
  Worker
} = require('worker_threads');

module.exports = class WS2SFWorker {
  constructor(){
    this.thread = new Worker('./lib/ws-sf-worker.js')
    this.toBeProcessed = []
    this.completionHandlers
  }

  registerCompletionHandler(id, handler){
    this.completionHandlers[id] = handler
  }

  async addToBeProcessed({ type, message }) {
    this.toBeProcessed.push({type, message})
    this.startWorking()
  }

  processMessage({type, message}){
    return new Promise((res, rej)=>{
      this.thread.postMessage({type, message})
      this.thread.on('message', res)
      this.thread.on('error', rej)
    }).then(() => {

    })
  }
  async startWorking(){
    while(this.toBeProcessed.length > 0) {
      let { message, type } = this.toBeProcessed[0]
      await this.processMessage({message, type})
      this.toBeProcessed.shift()
    }
    
  }
}
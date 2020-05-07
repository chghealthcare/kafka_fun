const WS2SFWorker = require('./WS2SFWorker')

module.exports = class Scheduler {
  constructor(){
    this.workers = []
    this.addWorker()
  }
  addWorker(){
    this.workers.push(new WS2SFWorker())
  }

  getMessageType(message){
    return 'create'
  }
 
  addWork(message){
    const type = this.getMessageType(message)
    this.workers[0].addToBeProcessed({type, message})
  }
}
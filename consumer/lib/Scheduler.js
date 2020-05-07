const WS2SFWorker = require('./WS2SFWorker')

module.exports = class Scheduler {
  constructor(){
    this.workers = []
    this.aggToWorker = {
       shift1: 1
    }
    this.aggToMessageId = {
      shift1: [ 1, 2, 3 ]
    }
    this.addWorker()
  }
  addWorker(){
    this.workers.push(new WS2SFWorker())
  }

  getLeastBusyWorker(aggId){

  }

  getWorkerIdByAgg() {
    // look up aggId,
    // if agg is not registered
    // get least busy worker
    // assign that worker to that aggId
  }

  getMessageType(message){
    return 'create'
  }
 
  addWork({key, offset, message}){
    const workerId = this.getWorkerIdByAgg(key)

    const type = this.getMessageType(message)
    this.workers[0].addToBeProcessed({type, message})
  }
}
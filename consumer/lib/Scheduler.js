const EventEmitter = require('events')
const WS2SFWorker = require('./WS2SFWorker')

module.exports = class Scheduler {
  constructor(){
    this.waitingQueue = {}
    this.processingQueue = []
    this.processingAggregateIds = {}
    this.workers = []
    this.WORKER_MAX = 9
    this.bus = new EventEmitter()
  }

  // Handler for new Kafka Event 
  handleNewKafkaEvent(message) {
    this.addToWaitingQueue(message);
    this.buildProcessingQueue();
  }

  // waitingQueue { - Organization of messages based on aggregate Id
  //    aggregateId1: [ - 
  //      messageId2
  //    ],
  //    aggregateId2: [
  //      messageId1,
  //      messageId2
  //    ],
  // }
  addToWaitingQueue(message) {
    if(this.waitingQueue[message.aggregateId]){
      this.waitingQueue[message.aggregateId].push(message)
    }else {
      this.waitingQueue[message.aggregateId] = [
        message
      ]
    }
}

  // send response to kafka
  respondToKafka() {
    // TODO
    // Tell Kafka that the event has been handled
  }

  initializeWorkerPool() {
    for( let i = 0; i < this.WORKER_MAX; i++){
      this.addWorker();
    }
  }

  // processingQueue is a list of messages ready to be processed sorted by aggregateId, looks like this
  //   processingQueue = [
  //     { message: msg, assigned: true },
  //     { message: msg, assigned: true },
  //     { message: msg, assigned: false },
  //     { message: msg, assigned: false },
  //     { message: msg, assigned: false },
  //   ]
  //
  // processingAggregateIds is a list of aggregate Ids currently being worked on, looks like this
  //   processingAggregateIds = {
  //     aggregateId1: true,
  //     aggregateId2: true
  //   }
  // 
  // This method transfers messages from the waitingQueue into the processingQueue & builds/updates the processingAggregateIds list
  buildProcessingQueue() {
    const aggregateIds = Object.keys(this.waitingQueue)
    // determines whether or not a message is releated another message already being worked on using the AggregateId
    for (const aggregateId of aggregateIds) {
<<<<<<< HEAD
      if(!this.processingAggregateIds[aggregateId]){
        // Add aggregateId to processingAggregateIds
        this.processingAggregateIds[aggregateId] = this.waitingQueue[aggregateId]
  
        const message = this.waitingQueue[aggregateId].shift();
        // clean up waitingQueue artifacts if there are no other messages for this aggregateId
        if (this.waitingQueue[aggregateId].length === 0) {
            delete this.waitingQueue[aggregateId]
        }
        
        // place available message into processingQueue
        this.processingQueue.push({ message, assigned: false})
        
        // TODO
        // Respond to Kafka letting it know the message is in the processing queue
=======
      if(!this.processingAggregateIds.includes(aggregateId)){
        //Add aggregateId to processingAggregateIds
        this.processingAggregateIds.push(aggregateId)

        const message = this.waitingQueue[aggregateId].shift();
        
        //place available message into processingQueue
        this.processingQueue.push(message)

        //Respond to Kafka letting it know the message is in the processing queue
>>>>>>> 052a5a83f087ca2084bac0e60504742a5c4e021c
      }
    }
    this.distributeMessagesToWorkers()
  }

  // Pull messages from processingQueue and hand off to workers
  distributeMessagesToWorkers() {
<<<<<<< HEAD
    // check that workers exist
    if(this.workers) {
      // hand off to first available worker
      this.workers.forEach (worker => {
        if(!worker.working){
          for (i = 0; i < this.processingQueue.length; i++){
            let event = this.processingQueue[i]
            if(!event.assigned) {
              // set to assigned
              event.assigned = true
              // hand off to worker
              worker.working = true
              worker.event = event
              this.workerProcessEvent(worker);
              break
            }
=======
    // Pull from processing queue and hand off to first available worker
    for (const worker in this.workers){
      if(!worker.working){
        for (const aggregateId in this.processingQueue){
          if(!aggregateId.assigned) {
            // set to assigned
            aggregateId.assigned = true
            // hand off to worker
            worker.working = true
            worker.message = aggregateId.message
            worker.startWorking();
            break
>>>>>>> 052a5a83f087ca2084bac0e60504742a5c4e021c
          }
        }
      })
    }
    else {
      this.addWorker()
      this.distributeMessagesToWorkers()
    }
<<<<<<< HEAD
=======
  }

  handleCompletedMessage(worker, message) {
    // respondToKafka()
    // Remove message from processing queue
    let index = processingQueue.indexOf(message.aggregateId);
    if (index > -1) { // add an error to be thrown because this should exist
      processingQueue.splice(index, 1);
    }

    // Remove AggregateId from processingAggregateIds
    delete processingAggregateIds[message.aggregateId]

    worker.working = false
>>>>>>> 052a5a83f087ca2084bac0e60504742a5c4e021c
  }

  addWorker(){
    // TODO in worker definition
    // these instances need to be constructed with a 'event' property that is null, working property that is false, and a assigned property that is false
    this.workers.push(new WS2SFWorker())
  }

  workerProcessEvent(worker) {
    // TODO
    // worker.startWorking()
    // timeout to simulate a call to salesforce which takes a long time
    setTimeout( () => {
        this.bus.emit('finished processing', worker)
    }, 500)
  }

  handleCompletedMessage(worker) {
    // TODO 
    // respondToKafka()
  
    // Remove message from processing queue
    let index = this.processingQueue.indexOf(worker.event);
    if (index > -1) { // add an error to be thrown because this should exist
      this.processingQueue.splice(index, 1);
    }
  
    // Remove AggregateId from processingAggregateIds
    delete this.processingAggregateIds[worker.event.message.aggregateId]
  
    // reset the worker metadata
    worker.working = false
    worker.event = {}
    this.buildProcessingQueue()
    this.distributeMessagesToWorkers();
  }
}
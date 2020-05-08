const WS2SFWorker = require('./WS2SFWorker')


/**
 * 
 * kafkaQueue [ - links kafka queue
 * ]
 * 
 * waitingQueue { - Organization of messages based on aggregate Id
 *  aggregateId1: [ - 
 *    messageId2
 *  ],
 *  aggregateId2: [
 *    messageId1,
 *    messageId2
 *  ],
 * }
 * 
 * processingAggregateId's: { - list of aggregate Ids currently being worked on
 *   aggregateId1: true,
 *   aggregateId2: true
 * }
 * 
 * processingQueue { - list of messages ready to be processed
 *  aggregateId: {
 *   message1: {},
 *   assigned: true
 *  },
 *  message2,
 *  message3,
 * }
 * 
 * 
 */

module.exports = class Scheduler {
  constructor(){
    this.runScheduler = true
    this.kafkaQueue = []
    this.waitingQueue = {}
    this.processingQueue = []
    this.processingAggregateIds = {}
    this.workers = []
    this.WORKER_MAX = 9
  }

  // Get queue from Kafka
  pullFromKafkaQueue() {
    // get Events from Kafka and tell Kafka we are working on it
  }

  // send response to kafka
  respondToKafka() {
    // Tell Kafka that the event has been handled
  }

  async startScheduler(){
    await this.initializeWorkerPool()
    /**
     * 
     * 
     * need to run both of these continuously at the same time
     * 
     * 
     */
    this.distributeMessagesToQueue()
    this.distributeMessagesToWorkers()
  }

  distributeMessagesToQueue() {
    this.pullFromKafkaQueue();
    this.buildWaitingQueue();
    this.buildProcessingQueue();
  }

  initializeWorkerPool() {
    for( let i = 0; i < this.WORKER_MAX; i++){
      this.addWorker();
    }
    
  }

  // transfers messages from kafkaQueue into waitingQueue
  // waitingQueue is composed of a list of aggregateIDs and related messages
  buildWaitingQueue() {
    this.kafkaQueue.forEach(msg => {
      if(waitingQueue[msg.aggregateId]){
        waitingQueue[msg.aggregateId].push(msg)
      }else {
        waitingQueue[msg.aggregateId] = [
          msg
        ]
      }
    })
  }

  /**
   * 
   * Set up kafka response to let kafka know that a message is now being processed
   * 
   * Possible have 3 statuses for events in kafka
   * pending, processing, completed
   * 
   * 
   */
  // transfers messages from waitingQueue into processingQueue
  buildProcessingQueue() {
    const aggregateIds = Object.keys(this.waitingQueue)
    // determines whether or not a message is releated another message already being worked on using the AggregateId
    for (const aggregateId of aggregateIds) {
      const messages = this.waitingQueue[aggregateId]
      if(!this.processingAggregateIds.includes(aggregateId)){
        
        //Add aggregateId to processingAggregateIds
        this.processingAggregateIds.push(aggregateId)

        const message = messages.shift();
        
        //place available message into processingQueue
        this.processingQueue.push(message)
      }
    }
  }



  /**
   * How do we best handle which message is already being processed
   * 
   * 
   * What happens if a worker working on a message crashes
   * 
   * 
   * When should we remove a message from the processing queue
   * 
   * 
   * 
   * //processingQueue
   *  {
   *    AggregateId: {
   *     Message
   *     status: 'completed'
   *    }
   *  }
   * 
   * //processingQueue
   *  [
   *    Message
   *  ]
   *  
   *  
   * 
   * 
   * 
   */

  // Pull messages from processQueue and hand off to workers
  distributeMessagesToWorkers() {
    // Pull from processing queue and hand off to first available worker
    for (const worker in this.workers){
      if(!worker.working){
        for (const aggregateId in this.processingQueue){
          if(!aggregateId.assigned) {
            // set to assigned
            // hand off to worker
          }
        }
      }
    }
  }

  handleCompletedMessage(message) {
    // Remove message from processing queue
    delete this.processingQueue[message.aggregateId]
    // Remove AggregateId from processingAggregateIds
    // respondToKafka()
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
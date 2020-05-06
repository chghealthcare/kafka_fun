const { Worker } = require('worker_threads')
const path = require('path')

const PROCESS_STATES = {
  STARTED: 'STARTED',
  PROCESSING: 'PROCESSING',
  STOPPED: 'STOPPED'
}
const shiftHandler = () =>
  new Promise(resolve => {
    setTimeout(() => {
      console.log(message)
      resolve()
    }, Math.floor(Math.random() * 3000))
  })

const waitForWorker = (worker, workerParams) => 
  new Promise(resolve => {
    worker.postMessage(workerParams)
    worker.once('message', result => {
      resolve({ result, worker })
    })
  })  

module.exports = class Scheduler {
  constructor(WORKER_POOL_SIZE = 9) {
    this.WORKER_POOL_SIZE = WORKER_POOL_SIZE
    this.STATE = PROCESS_STATES.STARTED

    this.workers = []
    this.working = []
    this.messageQueue = []
    this.dependentMessages = {}
    this.processingAggregates = {
      shiftId: 'PROCESSING'
    }

    this.initializeWorkers()
    this.start()
  }

  queueMessage(message) {
    this.messageQueue.push(message)
  
    if (this.STATE != PROCESS_STATES.PROCESSING) {
      this.start()
    }
  }

  removeMessage(message) {

  }

  initializeWorkers() {
    for (let i = 0; i < this.WORKER_POOL_SIZE; i++) {
      this.workers.push(new Worker(path.resolve(__dirname, './ws-sf-worker.js')))
    }
  }

  async start() {
    this.STATE = PROCESS_STATES.STARTED

    /**
     * 1. shift 1
     * 2. shift 2 
     * 3. shift 1
     * 4. shift 1
     * 5. shift 3
     * 6. shift 4
     * 7. shift 1
     */

    
  /**
   * const shiftsWorkedOn {
   * shiftId
   * }
   * 
   * 1. shift 1 - Processing
   * 2. shift 2 - Processing
   * 3. shift 1 - Processing (message 1)
   * 4. shift 1 - 
   * 5. shift 3 
   * 6. shift 4
   * 7. shift 1
   */

    /**
     * {
     *  shiftId1:  [
     *    3
     *    4
     *  ],
     * shiftId2:  [
     *    3
     *    4
     *  ],
     * }
     */

    /**
     * messages are processed in order (accuracy)
     * workers to keep working (efficiency, nothing is blocked where it doesn't have to be)
     * readable & maintainable
     */

    while (this.messageQueue.length > 0) {
      if (this.workers.length > 0) {

        const message = this.messageQueue[0]  // get the first message in queue
        // const { message, status } = queueItem
        if (this.processingAggregates[message.key]) { // if check to see if the aggregate id is being processed
          this.dependentMessages[message.key].push(message) // add current message to dependent messages
          /**
           * aggId: [ message 3, message 4]
           */
          this.messageQueue.shift() // remove current message from the top of the queue
          // 
        } else {
          if (this.dependentMessages[message.key].length > 0) {
            this.messageQueue.unshift(this.dependentMessages[message.key]) // put dependent at the front
          } else {
            // removed
            this.working.push(
              waitForWorker(nextAvailableWorker, { kafkaMessage: message, kafkaMessageHandler: shiftHandler })
                .then(({ result, worker }) => {
                  //const messageQueueIndex = this.messageQueue.findIndex(m => m.offset === message.offset)


                  delete this.processingAggregates[message.key]

                  this.workers.push(worker)
                  // remove current message from queue
                })
            )
          }
        }
       
        
          
          
        
        
        
        
        
        
        
        
        
        const message = this.messageQueue[0]
        const nextAvailableWorker = this.workers.shift()
  
        if (this.processingAggregates[message.key]) {
          this.processingAggregates[message.key] = 'PROCESSING'
  

        }
      } else {
        await Promise.race(this.working)
      }
    }
  }
}
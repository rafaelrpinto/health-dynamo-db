let bunyan = require('bunyan');
let AWS = require('aws-sdk');
let DynamoDBWrapper = require('dynamodb-wrapper');
let awsConfig = require('./aws-config');

// We only accept hospitals, clinics, emergencies, etc.
const IGNORED_FACILITY_TYPES = [
  'COOPERATIVA',
  'CENTRAL DE ',
  'TELESSAUDE',
  'VIGILANCIA',
  'N/A',
  'HOME CARE',
  'RESIDENCIAL',
  'PSICOSSOCIAL',
  'CONSULTORIO',
  'APOIO',
  'FARMACIA',
  'LABORATORIO',
  'PREVENCAO',
  'PARTO',
  'OFICINA ORTOPEDICA'
];

// logger configuration
let log = bunyan.createLogger({name: 'dynamodb-logger'});

// dynamo client
const dynamodb = new AWS.DynamoDB(awsConfig);

// create the worker
let dynamoDBWrapper = new DynamoDBWrapper(dynamodb, {
  maxRetries: 6,
  retryDelayOptions: {
    base: 100
  }
});

// retry listener
dynamoDBWrapper.events.on('retry', function(e) {
  log.warn(`An API call to DynamoDB.${e.method}() acting on table ${e.tableName} was throttled.
   Retry attempt #${e.retryCount} will occur after a delay of ${e.retryDelayMs}ms.`);
});

// consumed capacity listener
dynamoDBWrapper.events.on('consumedCapacity', function(e) {
  log.warn(`An API call to DynamoDB.${e.method}() consumed ${e.capacityType}`, JSON.stringify(e.consumedCapacity, null, 2));
});

let lastCount = 0;
// batch write progress listener
dynamoDBWrapper.events.on('batchGroupWritten', function(e) {
  // logs only after 500+ rows were inserted to avoid console flooding
  if (e.processedCount - lastCount > 500) {
    log.warn(`${e.processedCount} items sent to DynamoDB so far`);
    lastCount = e.processedCount;
  }
});

/**
 * Service responsible for operations related to health facilities.
 */
class HealthFacilitiesService {

  /**
   * Constructor.
   */
  constructor() {
    //controle vars
    this.ignored = 0;
    this.countSleeping = false;
    this.commitStarted = false;
    this.tablesCreated = false;
    this.writeRequests = new Map();
  }

  /**
   * Writes the facilities into dynamodb.
   */
  commit() {
    this.commitStarted = true;

    log.info(`Total facilities to be commited: ${this.writeRequests.size}`);
    log.info(`Total facilities ignored: ${this.ignored}`);

    let params = {
      RequestItems: {
        [awsConfig.facilities.tableName]: Array.from(this.writeRequests.values())
      },
      ReturnConsumedCapacity: 'NONE',
      ReturnItemCollectionMetrics: 'NONE'
    };

    return dynamoDBWrapper.batchWriteItem(params, {
      [awsConfig.facilities.tableName]: {
        partitionStrategy: 'EvenlyDistributedGroupWCU',
        targetGroupWCU: awsConfig.facilities.wcu,
        groupDelayMs: 150
      }
    });
  }

  /**
   * Creates the DyhamoDB table.
   */
  async createTables() {
    const params = {
      TableName: awsConfig.facilities.tableName,
      KeySchema: [
        {
          AttributeName: 'id',
          KeyType: 'HASH'
        }
      ],
      AttributeDefinitions: [
        {
          AttributeName: 'id',
          AttributeType: 'N'
        }
      ],
      ProvisionedThroughput: {
        ReadCapacityUnits: awsConfig.facilities.rcu,
        WriteCapacityUnits: awsConfig.facilities.wcu
      }
    };

    try {
      log.info('Creating tables...');
      let result = await dynamodb.createTable(params).promise();
      log.info('Tables created', {result});
    } catch (err) {
      if (err.code === 'ResourceInUseException') {
        log.warn('Table already exists, moving on...');
      } else {
        log.error('Error creating tables', {err});
        throw err;
      }
    }
  }

  /**
   * Adds a health facility to the queue.
   * @param  {Object} facility Facility to be saved.
   */
  addFacility(facility) {
    // checks if the facility type does not match any ignored type
    let isAccepted = IGNORED_FACILITY_TYPES.every(ignored => (facility.type.indexOf(ignored) === -1));
    if (isAccepted) {
      this.writeRequests.set(facility.id, {
        PutRequest: {
          Item: {
            id: {
              N: facility.id
            },
            ibge: {
              N: facility.ibge
            },
            name: {
              S: facility.name
            },
            businessName: {
              S: facility.businessName
            },
            type: {
              S: facility.type
            },
            openingHours: {
              S: facility.openingHours
            },
            phone: {
              S: facility.phone
            },
            latitude: {
              N: facility.latitude
            },
            longitude: {
              N: facility.longitude
            },
            'addess.street': {
              S: facility.address.street
            },
            'addess.number': {
              S: facility.address.number
            },
            'addess.neighborhood': {
              S: facility.address.neighborhood
            },
            'addess.postalCode': {
              S: facility.address.postalCode
            },
            'addess.state': {
              S: facility.address.state
            },
            'addess.city': {
              S: facility.address.city
            },
            services: {
              SS: facility.services
            }
          }
        }
      });
    } else {
      this.ignored++;
    }

    if (!this.countSleeping) {
      this.countSleeping = true;
      setTimeout(() => {
        if (!this.commitStarted) {
          this.countSleeping = false;
          log.info(`${this.writeRequests.size} facilities added to the queue and ${this.ignored} ignored so far...`);
        }
      }, 5000);
    }
  }
}

module.exports = HealthFacilitiesService;

let bunyan = require('bunyan');
let AWS = require('aws-sdk');
let ddbGeo = require('dynamodb-geo');
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

// dynamo setup
const dynamodb = new AWS.DynamoDB(awsConfig);
const geoDynamoConfig = new ddbGeo.GeoDataManagerConfiguration(dynamodb, awsConfig.facilities.tableName);
geoDynamoConfig.hashKeyLength = 7; //optimized for 1km radius queries
geoDynamoConfig.rangeKeyAttributeName = 'facilityId';
const geoTableManager = new ddbGeo.GeoDataManager(geoDynamoConfig);

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

    log.info(`Total facilities to be sent to DynamoDB: ${this.writeRequests.size}`);
    log.info(`Total facilities ignored: ${this.ignored}`);

    const BATCH_SIZE = 25;
    const WAIT_BETWEEN_BATCHES_MS = 700;
    const TOTAL_FACILITIES = this.writeRequests.size;

    let writeRequests = Array.from(this.writeRequests.values());
    let requestsSent = 0;

    // async function that will gradually send the facilities to dynamoDB
    async function resumeWriting() {
      const thisBatch = writeRequests.splice(0, BATCH_SIZE);
      if (thisBatch.length === 0) {
        // all facilities sent to dynamo
        return Promise.resolve();
      }

      // sends the facilities to dynamo
      await geoTableManager.batchWritePoints(thisBatch).promise();

      // updates the count
      requestsSent += thisBatch.length;
      log.info(`Facilities sent to DynamoDB ${requestsSent++}/${TOTAL_FACILITIES}`);

      // sleeps
      await new Promise(function(resolve) {
        setInterval(resolve, WAIT_BETWEEN_BATCHES_MS);
      });

      // next batch
      return resumeWriting();
    }

    // starts the process
    return resumeWriting();
  }

  /**
   * Creates the DyhamoDB table.
   */
  async createTables() {
    const createTableInput = ddbGeo.GeoTableUtil.getCreateTableRequest(geoDynamoConfig);
    createTableInput.ProvisionedThroughput = {
      ReadCapacityUnits: awsConfig.facilities.rcu,
      WriteCapacityUnits: awsConfig.facilities.wcu
    };

    try {
      log.info('Creating tables...');
      let result = await dynamodb.createTable(createTableInput).promise();
      await dynamodb.waitFor('tableExists', {TableName: awsConfig.facilities.tableName}).promise();
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
        RangeKeyValue: {
          S: facility.id
        },
        GeoPoint: {
          latitude: facility.latitude,
          longitude: facility.longitude
        },
        PutItemInput: {
          Item: {
            facilityId: {
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
            street: {
              S: facility.address.street
            },
            addressNumber: {
              S: facility.address.number
            },
            neighborhood: {
              S: facility.address.neighborhood
            },
            postalCode: {
              S: facility.address.postalCode
            },
            state: {
              S: facility.address.state
            },
            city: {
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

let fs = require('fs');
let csv = require('fast-csv');
let bunyan = require('bunyan');
let removeAccents = require('remove-accents');
// project dependencies
let HealthFacilitiesService = require('./service');

let log = bunyan.createLogger({name: 'parser-logger'});

/**
 * Defines the value as N/A if it's absent.
 */
function val(value) {
  if (!value) {
    return 'N/A';
  }
  return removeAccents(value).toUpperCase();
}

/**
 * Filters the medical service.
 * @param  {String} services Medical sevices separated by |.
 * @return {Array}          Array of medical services.
 */
function filterServices(services) {
  let array = val(services).split('|');
  array = array.map(obj => obj.trim());
  return Array.from(new Set(array).values());
}

/**
 * Class that parses a file containing health facilities.
 */
class HealthFacilitiesParser {

  /**
   * Constructor.
   * @param  {String} targetFile Path to the target CSV file.
   */
  constructor(targetFile) {
    this.targetFile = targetFile;
  }

  /**
   * Parses the file.
   * @return {Promise} A promise to resolve the file parsing.
   */
  parse() {
    let self = this;
    return new Promise(async(resolve, reject) => {
      // creates a service instance to store the facilities
      let service = new HealthFacilitiesService();

      // Reads the file
      let stream = fs.createReadStream(self.targetFile);

      // CSV parsing options
      let parsingOptions = {
        headers: true,
        delimiter: ',',
        quote: '"',
        objectMode: true,
        trim: true
      };

      // parses the CSV file
      let csvStream = csv(parsingOptions).on('data', (data) => {
        // transforms the data structure
        let healthFacility = {
          id: data.co_cnes,
          ibge: data.co_ibge,
          type: val(data.ds_tipo_unidade),
          openingHours: val(data.ds_turno_atendimento),
          services: filterServices(data.ds_servico_especializado),
          name: val(data.no_fantasia),
          businessName: val(data.no_razao_social),
          phone: val(data.nu_telefone),
          latitude: data.lat,
          longitude: data.long,
          address: {
            street: val(data.no_logradouro),
            number: val(data.nu_endereco),
            neighborhood: val(data.no_bairro),
            postalCode: val(data.co_cep),
            state: val(data.uf),
            city: val(data.municipio)
          }
        };

        service.addFacility(healthFacility);
      }).on('end', async() => {
        try {
          log.info('CSV parsed. Commiting to dynamodb...');
          await service.commit();
          log.info('Done');
          resolve();
        } catch (err) {
          reject(err);
        }
      });

      //creates the dynamodb tables
      await service.createTables();

      log.info('Parsing CSV....');
      // pipes the stream
      stream.pipe(csvStream);
    });
  }
}

module.exports = HealthFacilitiesParser;

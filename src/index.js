let bunyan = require('bunyan');
// project dependencies
let HealthFacilitiesParser = require('./parser');

let log = bunyan.createLogger({name: 'app-logger'});

async function parseHealthFacilities() {
  let parser = new HealthFacilitiesParser(process.argv[2]);
  try {
    log.info('Initiating process.');
    await parser.parse();
    log.info('Process complete.');
  } catch (err) {
    log.fatal({err});
  } finally {
    process.exit();
  }
}

parseHealthFacilities();

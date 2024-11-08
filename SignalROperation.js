const signalr = require('node-signalr');
const fs = require('fs');
const csv = require('csv-parser');
const config = require('./config/config_server1');
const logger = require('./utils/logger'); 

// Define the SignalR server URL and hubs
const serverUrl = config.signalR.serverURL;
const hubs = [config.signalR.serverHUB];

// Symbol pair lookup object
let symbolPairLookup = {};

// Load forex pairs from CSV file
async function loadSymbol() {
  return new Promise((resolve, reject) => {
    const lookup = {};
    fs.createReadStream('./config/symbol.csv')
      .pipe(csv())
      .on('data', (row) => {
        if (row.Symbol) {
          lookup[row.Symbol.trim()] = parseInt(row.BSymbolId, 10);
        }
      })
      .on('end', () => {
        logger.info('All Symbol Pairs loaded.');
        resolve(lookup);
      })
      .on('error', (error) => {
        logger.error('Error loading Symbol pairs:', error);
        reject(error);
      });
  });
}

// Function to check if a symbol is valid in CSV
function isValidPair(symbol) {
  return symbolPairLookup.hasOwnProperty(symbol);
}

// Create the SignalR client
const client = new signalr.Client(serverUrl, hubs);

// Configuration settings
client.callTimeout = 10000; // Timeout for sending messages: 10 seconds
client.reconnectDelayTime = 2000; // Delay time for reconnecting: 2 seconds

async function startSignalRClient(onPriceUpdate) {
  try {
    // Load forex pairs before starting the SignalR client
    symbolPairLookup = await loadSymbol();

    // Event: Connected to SignalR server
    client.on('connected', () => {
      logger.info('SignalR client connected.');
    });

    // Event: Reconnecting to SignalR server
    client.on('reconnecting', (retryCount) => {
      logger.info(`SignalR client reconnecting (Attempt #${retryCount})...`);
    });

    // Event: Disconnected from SignalR server
    client.on('disconnected', (reason) => {
      logger.info(`SignalR client disconnected due to: ${reason}`);
    });

    // Event: Error occurred during connection
    client.on('error', (error) => {
      logger.error(`SignalR client encountered an error: ${error.code}.`);
    });

    // Event: NotifyPriceUpdate received from the PriceBroadcasterHub
    client.connection.hub.on('PriceBroadcasterHub', 'NotifyPriceUpdate', (message) => {
      try {
        // Check that message is an object and has the required properties
        if (typeof message === 'object' && message.SymbolName && typeof message.Bid === 'number' && typeof message.Ask === 'number') {
          if (message.SymbolName.endsWith('.std') && isValidPair(message.SymbolName)) {
            const symbolName = message.SymbolName;
            const bidPrice = message.Bid; 
            const askPrice = message.Ask;
            const symbolId = symbolPairLookup[symbolName]; 
    
            if (symbolId) {
              onPriceUpdate(symbolName, symbolId, bidPrice, askPrice); // Trigger callback with extracted data
            } else {
              logger.warn(`SymbolId not found for symbol ${symbolName}`);
            }
          }
        } else {
          logger.warn('Received data does not have the expected structure:', message);
        }
      } catch (err) {
        logger.error('Error processing NotifyPriceUpdate:', err);
      }
    });    

    // Start the SignalR client
    client.start();
  } catch (error) {
    logger.error('Error starting SignalR client:', error);
  }
}

module.exports = { startSignalRClient };
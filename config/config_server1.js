// config/config_server1.js
require('dotenv').config();  // This will automatically look for .env in the project root

module.exports = {
  mssql: {
    user: process.env.MSSQL_USER,
    password: process.env.MSSQL_PASSWORD,
    server: process.env.MSSQL_SERVER,
    database: process.env.MSSQL_DATABASE,
    port: parseInt(process.env.MSSQL_PORT, 10) || 1433  // Ensure port is a number
  },
  api: {
    URL: process.env.API_SERVER1_URL
  },
  signalR: {
    serverURL: process.env.SIGNALR_SERVER_URL,
    serverHUB: process.env.SIGNALR_HUB
  }  
};

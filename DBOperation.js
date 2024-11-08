const sql = require('mssql');
const config = require(`./config/config_server1.js`);
const logger = require('./utils/logger'); 

const sqlConfig = {
    user: config.mssql.user,
    password: config.mssql.password,
    database: config.mssql.database,
    server: config.mssql.server,
    port: config.mssql.port,
    pool: {
        max: 20,
        min: 0,
        idleTimeoutMillis: 30000
    },
    options: {
        useUTC: true,
        trustServerCertificate: false,
        encrypt: false
    },
    requestTimeout: 30000, 
    connectionTimeout: 30000 
};

const MAX_RETRIES = 3;
const DEADLOCK_ERROR_CODE = '1205'; // SQL Server deadlock error code

let pool; // Declare pool globally and manage pool lifecycle externally
let activeDBOperations = 0;
let commandStep = '';
let isShuttingDown = false;

// Function to Initialize Database Connection 
async function initializeDBConnection() {
    if (!pool) {
        try {
            pool = await sql.connect(sqlConfig); // Create a shared connection pool
            logger.info("Database connection established.");
        } catch (err) {
            console.error("Error establishing database connection:", err);
        }
    }
};

// Function to Close Database Connection
async function closeDBConnection() {
    if (pool.connected) {
        await pool.close(); // Close the connection pool when finished
        pool = null; // Reset the pool variable after closing
    }
};

// Function to Reset Connection Pool 
async function resetConnectionPool() {
    try {
        // Ensure we are resetting a valid pool
        if (pool.connected) {
            logger.info('Resetting SQL connection pool...');
            await pool.close(); // Close existing pool
        } else {
            logger.info('Connection pool already closed, reconnecting...');
        }
        pool = await sql.connect(sqlConfig); // Reconnect with a new pool
        logger.info('Connection pool reset successfully.');
    } catch (err) {
        logger.error(`Error resetting connection pool: ${err}`);
        throw err;
    }
};

// Function to handle both SIGINT and SIGTERM signals
async function shutdown(signal) {
    logger.info(`Shutting Down: Closing database connection...`);

    isShuttingDown = true;
    // Wait for ongoing database operations to finish
    await waitForPendingOperations();

    if (pool) { // Check if the pool is initialized
        try {
            await closeDBConnection(); // Close the database connection
            logger.info("Database connection closed.");
        } catch (err) {
            console.error("Error closing the database connection:", err);
        }
    }
    
    process.exit(0); // Exit the process after cleanup
};

// Function to wait for all pending operations to complete before shutdown
async function waitForPendingOperations() {
   
    // Wait until there are no more active database operations
    while (activeDBOperations > 0) {
        logger.info(`Waiting for ${activeDBOperations} ongoing operations to complete...`);
        await new Promise(resolve => setTimeout(resolve, 500)); // Wait 0.5 second
    }

    logger.info("All database operations completed.");
};

// Reconnect wrapper for handling retries and pool reconnections
async function executeDBQuery(dbFunction, description = 'Executing DB operation') {
    if (isShuttingDown) throw new Error(`Shutting Down: Operation '${description}' has been halted.`);

    activeDBOperations++; // Increment before starting the operation
    let attempts = 0;

    try {
        while (attempts < MAX_RETRIES) {
            try {
                // Execute the database function
                return await dbFunction();
            } catch (err) {
                // Handling specific SQL error codes and connection issues
                if (err.number === DEADLOCK_ERROR_CODE) {
                    attempts++;
                    logger.warn(`Deadlock detected for ${description}. Retrying ${attempts}/${MAX_RETRIES}...`);
                    if (attempts >= MAX_RETRIES) {
                        logger.error(`Max retries reached due to deadlock for ${description}.`);
                        throw new Error('Max retries reached for deadlock.');
                    }
                    await new Promise(resolve => setTimeout(resolve, 5000)); // Delay before retrying
                } else if (['ETIMEOUT', 'ESOCKET', 'ECONNRESET', 'ECONNCLOSED', 'ENETRESET', 'ENOTOPEN', 'EPIPE', 'EHOSTUNREACH', 'ENETUNREACH'].includes(err.code)) {
                    attempts++;
                    logger.warn(`Connection error (${err.code}) at ${description}. Retrying ${attempts}/${MAX_RETRIES}...`);
                    if (attempts >= MAX_RETRIES) {
                        logger.error(`Max retries reached for ${description} due to connection issues.`);
                        throw new Error(`Max retries reached for connection errors at ${description}.`);
                    }
                    await resetConnectionPool(); // Reset the connection pool
                    await new Promise(resolve => setTimeout(resolve, 30000)); // Delay before retrying
                } else if (err.code === 'ELOGIN') {
                    attempts++;
                    logger.warn(`Login failed for ${description}. Retrying ${attempts}/${MAX_RETRIES}...`);
                    if (attempts >= MAX_RETRIES) {
                        logger.error(`Max retries reached due to login failure for ${description}.`);
                        throw new Error('Max retries reached for login failure.');
                    }
                    await new Promise(resolve => setTimeout(resolve, 30000)); // Delay before retrying
                } else if (err.code === 'EALREADYCONNECTED') {
                    logger.error(`Connection error for ${description}: already connected.`);
                    throw new Error('Connection already established.');
                } else if (err.code === 'EALREADYCONNECTING') {
                    logger.error(`Connection error for ${description}: already connecting.`);
                    throw new Error('Another connection attempt is in progress.');
                } else if (err.code === 'ER_ACCESS_DENIED_ERROR') {
                    logger.error(`Access denied error at ${description}: ${err}`);
                    throw new Error('Access denied: insufficient permissions.');
                } else if (err.code === 'EPROTOCOL') {
                    logger.error(`Protocol error at ${description}: ${err}`);
                    throw new Error('Protocol error detected.');
                } else {
                    // Log and rethrow other errors
                    logger.error(`Database error at ${description}: ${err}`);
                    throw err;
                }
            }
        }

        throw new Error(`Exceeded maximum retry attempts for ${description}.`);
    } finally {
        activeDBOperations--; // Decrement after the operation finishes (success or failure)
    }
};

// Function to fetch User Account (Trade API) from Database 
async function fetchUserAccount_TradeAPI() {
    if (isShuttingDown) throw new Error(`Shutting Down: Operation 'fetchUserAccount_TradeAPI' has been halted.`);

    const commandStep = 'Fetching User Account from Trade API';

    const queryFn = async () => {
        const userAccountData = await pool.request()
            .query(`SELECT 
                MemberId, 
                BExchangeId, 
                TradeApiType, 
                ApiKey,
                ApiSecret 
                FROM dbo.tbTradeApi
                WHERE TradeApiType = 'M'
                AND Status = 'A'`
            );

        if (userAccountData.recordset.length > 0) {
            return userAccountData.recordset.map(row => ({
                memberId: row.MemberId,
                bExchangeId: row.BExchangeId,
                tradeApiType: row.TradeApiType,
                apiKey: row.ApiKey,
                apiSecret: row.ApiSecret
            }));
        } else {
            return [];
        }
    };

    // Call wrapper function with retry and reconnect logic
    return await executeDBQuery(queryFn, commandStep);
};

// Function to Update User API Balance in Database
async function executeApiBalanceUpdate_TradeAPI(memberId, bExchangeId, tradeApiType, balance) {
    if (isShuttingDown) throw new Error(`Shutting Down: Operation 'executeApiBalanceUpdate_TradeAPI' for ${memberId} has been halted.`);

    const commandStep = `Executing API Balance Update for ${memberId}: ${balance}`;

    const queryFn = async () => {
        await pool.request()
            .input('MemberId', sql.BigInt, memberId)
            .input('BExchangeId', sql.Int, bExchangeId)
            .input('TradeApiType', sql.Char(1), tradeApiType)
            .input('AvailableBalance', sql.Decimal(16, 8), balance)
            .execute('[dbo].[spBot_ApiBalanceUpdate]');
    };

    // Call wrapper function with retry and reconnect logic
    return await executeDBQuery(queryFn, commandStep);
};

// Function to Update API Error in Database
async function executeApiErrorUpdate_TradeAPI(memberId, bExchangeId, tradeApiType, errorMsg) {
    if (isShuttingDown) throw new Error(`Shutting Down: Operation 'executeApiErrorUpdate_TradeAPI' for ${memberId} has been halted.`);

    const commandStep = `Executing API Error Update for ${memberId}`;

    const queryFn = async () => {
        await pool.request()
            .input('MemberId', sql.BigInt, memberId)
            .input('BExchangeId', sql.Int, bExchangeId)
            .input('TradeApiType', sql.Char(1), tradeApiType)
            .input('Remark', sql.NVarChar(sql.MAX), errorMsg)
            .execute('[dbo].[spBot_ApiErrorUpdate]');
    };

    // Call wrapper function with retry and reconnect logic
    return await executeDBQuery(queryFn, commandStep);
};

// Function to Update Symbol Price in Database
async function executeSymbolPriceUpdate(symbolName, symbolId, bidPrice, askPrice) {
    if (isShuttingDown) throw new Error(`Shutting Down: Operation 'executeSymbolPriceUpdate' for ${symbolName} has been halted.`);

    const commandStep = `Executing Symbol Price Update for ${symbolName} (ID: ${symbolId})`;
    //console.log(`PriceBid Value ${symbolName}: ${bidPrice}, Type: ${typeof bidPrice}`);

    const queryFn = async () => {
        const lastId = Date.now(); // Unix timestamp in milliseconds
        
        // Convert bidPrice and askPrice to strings
        const bidPriceStr = bidPrice.toString();
        const askPriceStr = askPrice.toString();

        return await pool.request()
            .input('BExchangeId', sql.Int, 10010) 
            .input('BSymbolId', sql.BigInt, symbolId)
            .input('PriceBid', sql.NVarChar, bidPriceStr) // Use NVarChar as workaround
            .input('PriceAsk', sql.NVarChar, askPriceStr) // Use NVarChar as workaround
            .input('LastId', sql.BigInt, lastId)
            .execute('[dbo].[spBot_SymbolPriceUpdate_FOREX_v4]');
    };

    return await executeDBQuery(queryFn, commandStep);
}


// Function to execute Trade Config for Opening Trade
async function executeTradeConfig(symbol, bSymbolId) {
    if (isShuttingDown) throw new Error(`Shutting Down: Operation 'executeTradeConfig' for ${symbol} has been halted.`);

    const commandStep = `Executing Trade Config for ${symbol}`;

    // Define the query function to execute
    const queryFn = async () => {
        const tradeConfigData = await pool.request()
            .input('TradeApiType', sql.Char(1), 'M') // Assuming 'M' is a fixed value
            .input('BSymbolId', sql.BigInt, bSymbolId)
            .execute('[dbo].[spBot_TradeConfigActivity_FOREX_v9]');

        if (tradeConfigData.recordset.length > 0) {
            const precision = 10000;
            return tradeConfigData.recordset.map(row => ({
                tConfigId: row.TConfigId,
                tCActivityId: row.TCActivityId !== null ? row.TCActivityId : -1,
                apiKey: row.ApiKey,
                apiSecret: row.ApiSecret,
                apiBalance: row.ApiBalance,
                cConfigId: row.CConfigId,
                clientMultiplier: row.ClientMultiplier,
                lastCycleId: row.LastCycleId,
                lastCycleRefCode: row.LastCycleRefCode,
                lastCycleSeq: row.LastCycleSeq !== null ? row.LastCycleSeq : -1,
                lastCyclePos: row.LastCyclePos !== null ? row.LastCyclePos : -1,
                lastCycleTime: row.LastCycleTime,
                lastCycleStatus: row.LastCycleStatus,
                lastCycleOrderId: row.LastCycleOrderId,
                lastCycleTradeId: row.LastCycleTradeId,
                memberId: row.MemberId,
                externalId: row.ExternalId,
                strategyMode: row.StrategyMode,
                orderSide: row.OrderSide,
                orderType_BuyIn: row.OrderType_BuyIn,
                orderType_SellOut: row.OrderType_SellOut,
                orderAmount: row.OrderAmount,
                orderSetup: (row.OrderSetup || '').split('·').map(size => parseFloat(size)),
                orderQuoteBegin: row.OrderQuoteBegin !== null ? row.OrderQuoteBegin : -1,
                orderQuoteEnd: row.OrderQuoteEnd !== null ? row.OrderQuoteEnd : -1,
                profitMethod: row.ProfitMethod,
                profitRate: row.ProfitRate / precision,
                profitRetraceRate: row.ProfitRetraceRate / precision,
                coverDownPattern: row.CoverDownPattern,
                coverDownSetup: row.CoverDownSetup,
                coverDownCount: row.CoverDownCount,
                coverdownMode: row.CoverdownMode,
                coverDownRate: (row.CoverDownRate || '').split('·').reduce((acc, rate, index) => (acc.push(parseFloat(rate) + (row.CoverDownPattern === 'E' && index > 0 ? acc[index - 1] : 0)), acc), []),
                coverDownQuote: (row.CoverDownQuote || '').split('·').map(size => parseFloat(size)),
                coverPullbackRate: row.CoverPullbackRate / precision,
                coverPullbackSetup: (row.CoverPullbackSetup || '').split('·').map(size => parseFloat(size)),
                cutLossRate: row.CutLossRate / precision,
                splitLastSeq: row.SplitLastSeq !== null ? row.SplitLastSeq : -1,
                cycleLimit: row.CycleLimit,
                commandType: row.CommandType !== null ? row.CommandType : ' ',
                commandArgs: row.CommandArgs,
                commandTrigger: row.CommandTrigger,
                isOrderX2: row.IsOrderX2 !== null ? row.IsOrderX2 : false,
                isAllowCoverdown: row.IsAllowCoverdown,
                buyEntryPrice: row.BuyEntryPrice !== null ? row.BuyEntryPrice : -1,
                buyAvgPrice: row.BuyAvgPrice !== null ? row.BuyAvgPrice : -1,
                buyQuantity: row.BuyQuantity !== null ? row.BuyQuantity : -1,
                lastAvgPrice: row.LastAvgPrice !== null ? row.LastAvgPrice : -1,
                lastBuyQuantity: row.LastBuyQuantity !== null ? row.LastBuyQuantity : -1,
                lastSellSeq: row.LastSellSeq !== null ? row.LastSellSeq : -1,
                lastPriceLow: row.LastPriceLow !== null ? row.LastPriceLow : -1,
                lastPriceHigh: row.LastPriceHigh !== null ? row.LastPriceHigh : -1,
            }));
        } else {
            //logger.warn(`No trade configuration data returned for ${bSymbolId}.`);
            return []; // Log when no data is returned
        }
    };

    // Call the executeDBQuery wrapper with the queryFn and commandStep
    return await executeDBQuery(queryFn, commandStep);
};

// Function to execute Trade Cycle for Open Position
async function executeTradeCycleBuy(CConfigId, TConfigId, Seq, OrderType, OrderPrice, OrderQuantity, Debug, Status, OrderId, ClientOrderId, TCActivityId) {
    if (isShuttingDown) throw new Error(`Shutting Down: Operation 'executeTradeCycleBuy' for ${CConfigId} has been halted.`);

    commandStep = `Executing Trade Cycle Buy`;

    // Function to encapsulate the DB request
    const queryFn = async () => {
        const result = await pool.request()
            .input('CConfigId', sql.BigInt, CConfigId)
            .input('TConfigId', sql.BigInt, TConfigId)
            .input('Seq', sql.TinyInt, Seq)
            .input('OrderType', sql.Char(1), OrderType)
            .input('OrderPrice', sql.Decimal(30, 15), OrderPrice)
            .input('OrderQuantity', sql.Decimal(30, 15), OrderQuantity)
            .input('Debug', sql.NVarChar(500), Debug)
            .input('Status', sql.Char(1), Status)
            .input('OrderId', sql.VarChar(50), OrderId)
            .input('ClientOrderId', sql.VarChar(50), ClientOrderId)
            .input('TCActivityId', sql.BigInt, TCActivityId)
            .execute('[dbo].[spBot_TradeCycleBuy_FOREX_v9]');

        // Handle the result
        if (result.recordset && result.recordset.length > 0) {
            const response = result.recordset[0];
            if (response.ERROR) {
                console.error('Error:', response.ERROR);
                throw new Error(response.ERROR); // Throw an error if there is an issue
            }
            return response.ID;
        }
        return null; // In case of empty result set
    };

    return await executeDBQuery(queryFn, commandStep);
};

// Function to execute Trade Cycle for Close Position 
async function executeTradeCycleSell(CConfigId, TConfigId, OrderType, OrderPrice, OrderQuantity, Debug, Status, OrderId, ClientOrderId, TCActivityId) {
    if (isShuttingDown) throw new Error(`Shutting Down: Operation 'executeTradeCycleSell' for ${CConfigId} has been halted.`);

    commandStep = `Executing Trade Cycle Sell`;

    // Function to encapsulate the DB request
    const queryFn = async () => {
        const result = await pool.request()
            .input('CConfigId', sql.BigInt, CConfigId)
            .input('TConfigId', sql.BigInt, TConfigId)
            .input('OrderType', sql.Char(1), OrderType)
            .input('OrderPrice', sql.Decimal(30, 15), OrderPrice)
            .input('OrderQuantity', sql.Decimal(30, 15), OrderQuantity)
            .input('Debug', sql.NVarChar(500), Debug)
            .input('Status', sql.Char(1), Status)
            .input('OrderId', sql.VarChar(50), OrderId)
            .input('ClientOrderId', sql.VarChar(50), ClientOrderId)
            .input('TCActivityId', sql.BigInt, TCActivityId)
            .execute('[dbo].[spBot_TradeCycleSell_FOREX_v9]');

        // Handle the result
        if (result.recordset && result.recordset.length > 0) {
            const response = result.recordset[0];
            if (response.ERROR) {
                console.error('Error:', response.ERROR);
                throw new Error(response.ERROR); // Throw an error if there is an issue
            }
            return response.ID;
        }
        return null; // In case of empty result set
    };

    return await executeDBQuery(queryFn, commandStep);
};

// Function to fetch Trade Cycle Data
async function fetchTradeCycle() {
    if (isShuttingDown) throw new Error(`Shutting Down: Operation 'fetchTradeCycle' has been halted.`);

    commandStep = 'Fetching Trade Cycle Data';

    const queryFn = async () => {
        const tradeCycleData = await pool.request()
            .query(`
                SELECT 
                    TConfig.MemberId,
                    TradeApi.ApiKey,
                    BotSymbol.Code,
                    TCycle.TCycleId, 
                    TCycle.TConfigId,
                    TCycle.DateEntry,
                    TCycle.DateClose,
                    TCycle.BuyEntryPrice,
                    TCycle.BuyAvgPrice,
                    TCycle.BuyQuantity,
                    TCycle.SellAvgPrice,
                    TCycle.SellQuantity,
                    TCycle.SellEarning,
                    TCycle.Status,
                    TCycle.Side
                FROM dbo.tbTradeCycle AS TCycle
                INNER JOIN dbo.tbTradeConfig AS TConfig ON TCycle.TConfigId = TConfig.TConfigId
                INNER JOIN dbo.tbTradeApi AS TradeApi ON TConfig.MemberId = TradeApi.MemberId
                LEFT JOIN dbo._tbBotSymbol AS BotSymbol ON TConfig.BsymbolId = BotSymbol.BsymbolId
            `);

        if (tradeCycleData.recordset.length > 0) {
            return tradeCycleData.recordset.map(row => ({
                memberId: row.MemberId,
                apiKey: row.ApiKey,        
                symbol: row.Code,
                tCycleId: row.TCycleId,
                tConfigId: row.TConfigId,
                dateEntry: row.DateEntry,
                dateClose: row.DateClose,
                buyEntryPrice: row.BuyEntryPrice,
                buyAvgPrice: row.BuyAvgPrice,
                buyQuantity: row.BuyQuantity,
                sellAvgPrice: row.SellAvgPrice,
                sellQuantity: row.SellQuantity,
                sellEarning: row.SellEarning,
                status: row.Status,
                side: row.Side                              
            }));
        } else {
            logger.warn('No Trade Cycle data found.');
            return [];
        }
    };

    return await executeDBQuery(queryFn, commandStep);
};

// Function to fetch Trade Cycle Detail Data
async function fetchTradeCycleDetail(tCycleId) {
    if (isShuttingDown) throw new Error(`Shutting Down: Operation 'fetchTradeCycleDetail' has been halted.`);

    commandStep = 'Fetching Trade Cycle Detail Data';
    
    const queryFn = async () => {
        const tradeCycleData = await pool.request()
            .input('TCycleId', sql.BigInt, tCycleId)
            .query(`SELECT 
                [TCycleId],
                [Seq],
                [Pos],
                [TimeAction],
                [OrderCost],
                [PriceFill],
                [QuantityFill],
                [Debug],
                [TCActivityId],
                [Status],
                [ref_ClientOrderId],
                [ref_OrderId],
                [ref_TradeId],
                [PriceLow],
                [PriceHigh],
                [OrderType],
                [FeeCode],
                [FeePrice],
                [FeeQuantity],
                [OrderPrice],
                [OrderQuantity]
            FROM [tb_cerabot].[dbo].[tbTradeCycle_Detail]
            WHERE [TCycleId] = @TCycleId`);

        if (tradeCycleData.recordset.length > 0) {
            return tradeCycleData.recordset.map(row => ({
                tCycleId: row.TCycleId,
                orderId: row.ref_OrderId              
            }));
        } else {
            logger.warn('No Trade Cycle Detail data found.');
            return [];
        }
    };

    return await executeDBQuery(queryFn, commandStep);
};

// Function to Update Profit for Trade Cycle in Database 
async function executeTradeCycleProfitUpdate(memberId, tCycleId, profitAmount) {
    if (isShuttingDown) throw new Error(`Shutting Down: Operation 'executeTradeCycleProfitUpdate' for TCycle Id ${tCycleId} has been halted.`);

    commandStep =  `Executing Trade Cycle ${tCycleId} Profit Update`
    
    const queryFn = async () => {
        const result = await pool.request()
        .input('MemberId', sql.BigInt, memberId)
        .input('TCycleId', sql.BigInt, tCycleId)
        .input('ProfitAmount', sql.Decimal(35, 15), profitAmount)
        .execute('[dbo].[spBot_TradeCycleProfitUpdate_FOREX_v9]');
    
        // Handle the result
        if (result.recordset && result.recordset.length > 0) {
            const response = result.recordset[0];
            if (response.ERROR) {
                console.error('Error:', response.ERROR);
                throw new Error(response.ERROR); // Throw an error if there is an issue
            }
            return response.ID;
        }
        return null; // In case of empty result set
    };

    return await executeDBQuery(queryFn, commandStep);
};

// Function to Update Trade Income in Database 
async function executeTradeIncomeMaintenance(memberId, tCycleId, incomeType, value, orderId) {
    if (isShuttingDown) throw new Error(`Shutting Down: Operation 'executeTradeIncomeMaintenance' for Order ${orderId} has been halted.`);

    commandStep =  `Executing Trade Income for Order ${orderId} Maintenance`
    
    const queryFn = async () => {
        await pool.request()
        .input('MemberId', sql.BigInt, memberId)
        .input('TCycleId', sql.BigInt, tCycleId)
        .input('IncomeType', sql.Char(1), incomeType)
        .input('Value', sql.Decimal(16, 8), value)
        .input('OrderId', sql.VarChar(50), orderId)
        .execute('[dbo].[spBot_TradeIncomeMaintenance_FOREX]');

        logger.info('Trade Income Updated into DB successfully.');       
        return;
    };

    return await executeDBQuery(queryFn, commandStep);
};

// Function to execute Trade Activity Error 
async function executeTradeActivityError(CConfigId, TConfigId, Remark) {
    if (isShuttingDown) throw new Error(`Shutting Down: Operation 'executeTradeActivityError' for ${TConfigId} has been halted.`);

    commandStep =  `Executing Trade Activity Error`;

    const queryFn = async () => {
        await pool.request()
        .input('CConfigId', sql.BigInt, CConfigId)
        .input('TConfigId', sql.BigInt, TConfigId)
        .input('Remark', sql.NVarChar(500), Remark)
        .execute('[dbo].[spBot_TradeActivity_Error_FOREX_v9]');

        logger.info('Trade Activity Error logged into DB successfully.');
        return;
    };

    return await executeDBQuery(queryFn, commandStep);
};

// Function to execute Open Position Fail
async function executeBuyFail(CConfigId, TConfigId, Remark, TCActivityId) {
    if (isShuttingDown) throw new Error(`Shutting Down: Operation 'executeBuyFail' for ${TConfigId} has been halted.`);

    commandStep =  `Executing Buy Fail`;

    const queryFn = async () => {
        await pool.request()
        .input('CConfigId', sql.BigInt, CConfigId)
        .input('TConfigId', sql.BigInt, TConfigId)
        .input('Remark', sql.NVarChar(500), Remark)
        .input('TCActivityId', sql.BigInt, TCActivityId)
        .execute('[dbo].[spBot_TradeActivity_BuyFail_FOREX_v9]');

        logger.info('Buy fail logged into DB successfully.');
        return;
    };

    return await executeDBQuery(queryFn, commandStep);
};

// Function to execute Close Position Fail 
async function executeSellFail(CConfigId, TConfigId, Remark, TCActivityId) {
    if (isShuttingDown) throw new Error(`Shutting Down: Operation 'executeSellFail' for ${CConfigId} has been halted.`);

    commandStep =  `Executing Sell Fail`;

    const queryFn = async () => {
        await pool.request()
        .input('CConfigId', sql.BigInt, CConfigId)
        .input('TConfigId', sql.BigInt, TConfigId)
        .input('Remark', sql.NVarChar(500), Remark)
        .input('TCActivityId', sql.BigInt, TCActivityId)
        .execute('[dbo].[spBot_TradeActivity_SellFail_FOREX_v9]');

        logger.info('Sell fail logged into DB successfully.');
        return;                                     
    };

    return await executeDBQuery(queryFn, commandStep);
};

module.exports = { 
    initializeDBConnection,
    fetchUserAccount_TradeAPI,
    executeApiBalanceUpdate_TradeAPI,
    executeApiErrorUpdate_TradeAPI,
    executeSymbolPriceUpdate,
    executeTradeConfig, 
    executeTradeCycleBuy, 
    executeTradeCycleSell, 
    fetchTradeCycle,
    fetchTradeCycleDetail,
    executeTradeCycleProfitUpdate,
    executeTradeIncomeMaintenance,
    executeTradeActivityError,
    executeBuyFail, 
    executeSellFail,
    shutdown,
    isShuttingDown
};
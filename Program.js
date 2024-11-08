const { startSignalRClient } = require('./SignalROperation');
const { initializeDBConnection, fetchUserAccount_TradeAPI, executeApiBalanceUpdate_TradeAPI, executeApiErrorUpdate_TradeAPI, executeSymbolPriceUpdate, executeTradeConfig, executeTradeCycleBuy, executeTradeCycleSell, fetchTradeCycle, fetchTradeCycleDetail, executeTradeCycleProfitUpdate, executeTradeIncomeMaintenance, executeTradeActivityError, executeBuyFail, executeSellFail, shutdown, isShuttingDown } = require('./DBOperation');
const { getUserList, getUserData, getOpenTrades, orderOpen, orderClose, getTradeData } = require('./APIOperation');
const { validateApiKey } = require('./utils/validateApiKey');
const logger = require('./utils/logger'); 
const schedule = require('node-schedule');

// Declare Server Users globally
let users = [];
  
let isBusy = false;

const BATCH_SIZE = 10;

// Helper function for delays
function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
};

// Function to update the user list every 30 minutes
const updateUserList = async () => {
    // Run the getUserList function in the background every 30 minutes
    setInterval(async () => {
        try {
            // Fetch the latest user list and update the global `users`
            users = await getUserList();
            logger.info('User list updated.');
        } catch (err) {
            logger.error('Error updating user list:', err);
        }
    }, 1800000); // 30 minutes in milliseconds
};

// Function to start a loop that updates user account balances every 30 seconds
async function updateUserAccountBalances() {
    let hasLoggedNoUserData = false; // Flag to track if log message has been printed

    while (!isShuttingDown) {
        try {
            const allUsers = await fetchUserAccount_TradeAPI(); // Fetch all user accounts

            // Validate API keys for all users asynchronously
            const validationPromises = allUsers.map(async (user) => {
                const validation = await validateApiKey(user.apiKey);
                if (!validation.valid) {
                    logger.info(`API Key validation failed for API Key: ${user.apiKey}. Invalid API Key`);
                    const errorMsg = `API Key validation failed: Invalid API Key`;

                    await executeApiErrorUpdate_TradeAPI(
                        user.memberId,
                        user.bExchangeId,
                        user.tradeApiType,
                        errorMsg
                    );
                }
                
                return validation.valid; // Return validation result
            });

            // Wait for all API key validation promises
            await Promise.all(validationPromises);

            // Create a Set of valid user logins for faster lookups
            const validLogins = new Set(users.map(user => user.Login.toString()));

            // Filter only users whose API key is valid (based on validLogins set)
            const serverUsers = allUsers.filter(user => validLogins.has(user.apiKey));

            if (serverUsers.length === 0) {
                if (!hasLoggedNoUserData) {
                    logger.warn('No User Account data found for the specified users.');
                    hasLoggedNoUserData = true; // Avoid logging repeatedly
                }
                await delay(30000); // Wait before trying again
                continue; // Skip to the next iteration if no users are found
            } else {
                hasLoggedNoUserData = false;
            }

            // Fetch user data for valid users
            const responses = await getUserData(serverUsers);

            // Process each response
            for (const response of responses) {
                // Case-insensitive comparison for `name` and `apiSecret`
                if (response.name.toLowerCase() !== response.apiSecret.toLowerCase()) {
                    logger.info(`API Secret Key validation failed for login ID: ${response.login}. Incorrect Secret Key`);
                    const errorMsg = `API credentials validation failed: Incorrect Secret Key`;

                    await executeApiErrorUpdate_TradeAPI(
                        response.memberId,
                        response.bExchangeId,
                        response.tradeApiType,
                        errorMsg
                    );
                } else if (!response.error) { // Proceed only if no error is found in response
                    // Proceed to update the balance if no errors
                    await executeApiBalanceUpdate_TradeAPI(
                        response.memberId,
                        response.bExchangeId,
                        response.tradeApiType,
                        response.balance
                    );
                } else {
                    logger.error(`Error with user ${response.login}: ${response.error}`);
                }
            }
        } catch (err) {
            if (err.message.includes('Shutting Down')) {
                await delay(1000); // Wait and try again if shutting down
            } else {
                logger.error('Error in updateUserAccountBalances:', err.message); // Log error for debugging
            }
        }
        await delay(30000); // 30-second delay
    }
};

// Function to update swap fee in the database for all open trades
async function updateSwapFee() {
    const incomeType = 'S'; // For SwapFee

    try {
        // Fetch trade cycle data
        const tradeCycles = await fetchTradeCycle();

        // Filter active trade cycles
        const activeCycles = tradeCycles.filter(cycle => cycle.status === 'A');

        // Create a Set of valid user logins for faster lookup
        const validLogins = new Set(users.map(user => user.Login.toString()));

        // Iterate through active cycles to get open trades
        for (const cycle of activeCycles) {
            // Filter to only include cycles whose API key matches a valid login
            if (validLogins.has(cycle.apiKey)) {
                // Call getOpenTrades for each active cycle's API key
                const openTrades = await getOpenTrades(cycle.apiKey);

                // Iterate through open trades and update the swap fee for each
                for (const trade of openTrades) {
                    const memberId = cycle.memberId; // Get memberId from cycle
                    const tCycleId = cycle.tCycleId; // Get tCycleId from cycle
                    const value = trade.Swap; // Use the Swap value from the trade
                    const orderId = trade.OrderId.toString(); // Use the Order Id from the trade

                    if (value !== 0) {
                        await executeTradeIncomeMaintenance(memberId, tCycleId, incomeType, value, orderId);
                        logger.info(`Updated swap fee for Order ID: ${orderId} with value: ${value}`);
                    }
                }
            }
        }

        logger.info('All swap fees updated successfully.');
    } catch (error) {
        logger.error('Error updating swap fee:', error);
    }
};

// Function to schedule the swap fee update
function scheduleSwapFeeUpdate() {
    const nextExecutionDate = new Date();
    nextExecutionDate.setUTCHours(3, 30, 0, 0); // Set time to 03:30 UTC

    // If the scheduled time has already passed today, schedule for tomorrow
    if (nextExecutionDate < new Date()) {
        nextExecutionDate.setUTCDate(nextExecutionDate.getUTCDate() + 1); // Move to the next day if already passed
    }

    // Schedule the job to run daily at the specified time
    schedule.scheduleJob(nextExecutionDate, updateSwapFee);
};

// Unified function to log trade cycle and open trades based on trade status
async function logTradingData(tradeStatus) {
    const tradeCycleData = await fetchTradeCycle();

    if (!tradeCycleData || tradeCycleData.length === 0) {
        logger.info('No trade cycle data available at the moment.');
        return;
    }

    let filteredTradeCycle;

    // Filter trade cycles based on trade status ('A' = Active, 'P' = Closed)
    if (tradeStatus === 'A') {
        filteredTradeCycle = tradeCycleData.filter(cycle => cycle.status === 'A');
    } else if (tradeStatus === 'P') {
        const latestTradeCycle = tradeCycleData.reduce((latest, current) => {
            return new Date(current.dateClose) > new Date(latest.dateClose) ? current : latest;
        }, tradeCycleData[0]);  
        filteredTradeCycle = [latestTradeCycle];
    }

    if (filteredTradeCycle.length === 0) return;

    // Iterate through all users
    for (let user of users) {
        const apiKey = user.Login;

        // Filter trade cycles for the current user apiKey
        const userTradeCycles = filteredTradeCycle.filter(trade => trade.apiKey == apiKey);
        
        if (userTradeCycles.length === 0) continue; // Skip if no trade cycles for this apiKey

        console.log('');
        logger.info(`Trade Data for Login ${apiKey}`)

        // For each trade cycle of this apiKey, log the cycle and matching open trades
        for (let trade of userTradeCycles) {
            const formattedTradeCycle = {
                MemberId: trade.memberId,
                Login: apiKey,
                Symbol: trade.symbol,
                TCycleId: trade.tCycleId,
                TConfigId: trade.tConfigId,
                DateEntry: trade.dateEntry,
                DateClose: trade.dateClose,
                BuyEntryPrice: trade.buyEntryPrice,
                BuyAvgPrice: trade.buyAvgPrice,
                BuyQuantity: trade.buyQuantity,
                SellAvgPrice: trade.sellAvgPrice,
                SellQuantity: trade.sellQuantity,
                SellEarning: trade.sellEarning,
                Status: trade.status,
                Side: trade.side
            };

            // Log trade cycle data
            logger.info(`${formattedTradeCycle.Symbol} Cycle (Status: ${tradeStatus === 'A' ? 'Active' : 'Closed'}): ` + JSON.stringify(formattedTradeCycle, null, 2));

            // Fetch and log open trades for this trade cycle symbol
            const openTrades = await getOpenTrades(apiKey);
            const matchingOpenTrades = openTrades.filter(openTrade => openTrade.Symbol === trade.symbol);

            if (matchingOpenTrades.length > 0) {
                const formattedTrades = matchingOpenTrades.map(({ Symbol, OrderId, OpenPrice, Volume, Profit, Comment }) => ({
                    Symbol, OrderId, OpenPrice, Volume, Profit, Comment
                }));

                // Log the matching open trades
                logger.file(JSON.stringify({ login: apiKey, trades: formattedTrades }, null, 2));
                console.table(formattedTrades);
            }
        }
    }
};

// Function to generate Ref Code 
function generateRefCode() {
    // DCA #12345678 IN
    // DCA #12345678 CD 1
    // DCA #12345678 CD 2
    // DCA #12345678 CD MANUAL
    // DCA #12345678 TP
    // DCA #12345678 SL
    // DCA #12345678 SELL MANUAL
    const randomChar = (chars) => chars[Math.floor(Math.random() * chars.length)];
    const letters = Array.from({ length: 3 }, () => randomChar('ABCDEFGHIJKLMNOPQRSTUVWXYZ')).join('');
    const numbers = Array.from({ length: 5 }, () => randomChar('0123456789')).join('');
    return (letters + numbers).split('').sort(() => Math.random() - 0.5).join('');
};

// Function to get Open Price same as MT4
async function getOpenPrice(apiKey, orderId) {
    try {
        const tradeData = await getTradeData(apiKey, orderId);

        if (tradeData && tradeData.length > 0) {
            const openPrice = tradeData[0].OpenPrice !== undefined ? tradeData[0].OpenPrice : 0;
            return  openPrice;
        } else {
            logger.warn(`No trade data found for orderId: ${orderId}`);
            return 0; 
        }
    } catch (error) {
        logger.error(`Error getting Open Price for ${orderId}: `, error);
        return 0;  // Default to 0 open price in case of API failure
    }
};

// Function to calculate total profit for a trade cycle
async function calculateTotalProfit(apiKey, tCycleId) {
    try {
        // Fetch trade cycle details from the database
        const tradeCycleDetails = await fetchTradeCycleDetail(tCycleId);

        if (!tradeCycleDetails || tradeCycleDetails.length === 0) {
            logger.warn(`No trade cycle details found for tCycleId: ${tCycleId}`);
            return { totalProfit: 0 };  // Return 0 if no trade details found
        }

        // Collect all orderIds for batch processing
        const orderIds = tradeCycleDetails.map(trade => trade.orderId);
        const orderIdsString = orderIds.join(',');

        // Fetch trade data for all orderIds in a batch
        const tradeDataArray = await getTradeData(apiKey, orderIdsString);

        // Process trade data and calculate profits
        const tradeDetailsWithProfit = tradeCycleDetails.map((trade, index) => {
            const tradeData = tradeDataArray[index] || [];  // Safe access to trade data array
            const profit = tradeData ? Number(tradeData.Profit) || 0 : 0;
            const swap = tradeData ? Number(tradeData.Storage) || 0 : 0;

            return {
                tCycleId: trade.tCycleId,
                orderId: trade.orderId,
                profit,
                swap
            };
        });

        // Calculate total profit by summing the profit from all trades
        const totalProfit = tradeDetailsWithProfit.reduce((sum, trade) => sum + trade.profit, 0);
        
        return totalProfit;  // Total profit from all orders

    } catch (error) {
        logger.error(`Failed to get trade cycle details and calculate total profit: ${error.message}`);
        throw error;
    }
};

// Function to Open Position
async function PositionOpen(tc, tCActivityId, symbol, pnlRate, lastPrice, openSide, msgDebug) {
    // Determine sequence number
    const seq = tCActivityId > 0 ? tc.lastCycleSeq : (tc.lastCycleSeq + 1);

    // Initialize manual buy amount and price
    let manualBuyAmount = NaN;
    let manualBuyPrice = NaN;

    // Extract manual buy amount and price if applicable
    if (tCActivityId > 0 && tc.commandArgs) {
        const argsParts = tc.commandArgs.split('@');
        manualBuyAmount = argsParts.length > 0 ? parseFloat(argsParts[0]) : NaN;
        manualBuyPrice = argsParts.length > 1 ? parseFloat(argsParts[1]) : NaN;
    }

    // Determine order amount based on strategy and conditions
    let orderAmt = tCActivityId > 0
        ? manualBuyAmount
        : tc.strategyMode === '2'
            ? tc.orderAmount
            : (tc.strategyMode === '1' || tc.strategyMode === '0') && seq === 0
                ? tc.orderAmount
                : tc.orderSetup[tc.lastCycleSeq] || 0; // Default to 0 if undefined

    // Follow Setting, Override Amount if needed
    if (tc.cConfigId > 0 && tCActivityId > 0) {
        orderAmt = Math.floor(orderAmt * tc.clientMultiplier);
    }

    // Entry Order X2 Config
    if (seq === 0 && tc.isOrderX2 && tCActivityId <= 0) {
        orderAmt *= 2;
    }

    // Verify Wallet Balance
    if (orderAmt > tc.apiBalance) {
        await executeBuyFail(tc.cConfigId, tc.tConfigId, "[BOT] Insufficient Binance Wallet Balance", tCActivityId);
        logger.error(`OPEN FAIL ${tCActivityId > 0 ? "MANUAL " : ""}Order ${tc.memberId}: Insufficient Wallet Balance (Wallet ${tc.apiBalance}, Order ${orderAmt})`);
        return false;
    }

    // Initialize Cycle Reference Code if needed
    if (tc.lastCycleSeq === -1) {
        tc.lastCycleRefCode = generateRefCode();
    }

    // Calculate order quantity and volume
    const orderQty = Number.isInteger(orderAmt) ? orderAmt / 100 : orderAmt; // Lot size
    const volume = Math.round(orderQty * 100); // Volume

    // Determine order type and price
    const ordertype = (manualBuyPrice && manualBuyPrice > 0) ? 'L' : tc.orderType_BuyIn;
    const openPrice = (tCActivityId > 0 && manualBuyPrice > 0) ? manualBuyPrice : lastPrice;

    const contractSize = 100000; // Contract size constant
    let apiError = '';

    try {
        // Open Order
        const ticket = await orderOpen(tc.apiKey, openSide, volume, symbol, ordertype, openPrice, `DCA #${tc.lastCycleRefCode} ${tc.lastCycleSeq === -1 ? "IN" : `CD ${tCActivityId > 0 ? 'MANUAL' : tc.lastCycleSeq + 1}`}`);

        // Handle successful order open
        if (ticket) {
            const status = 'F';
            const orderId = ticket.toString();
            const actualPrice = await getOpenPrice(tc.apiKey, orderId);
            const actualQty = orderQty;
            const actualCost = Math.floor(orderQty * contractSize);

            // Log and debug messages
            let debugMsg = `${tCActivityId > 0 ? "MANUAL " : ""}${tc.lastCycleSeq === -1 ? `${openSide === 'B' ? 'BUY' : 'SELL'} OPEN ENTRY` : `${openSide === 'B' ? 'BUY' : 'SELL'} IN SEQ ${seq} @ ${tc.coverdownMode === 'R' ? (pnlRate * 100).toFixed(4) : pnlRate}`}`;
            let debugMsg2 = `${symbol}: [Price: ${actualPrice} Qty: ${actualQty} Cost: ${actualCost}]`;
            logger.info(debugMsg);
            logger.info(debugMsg2);
            debugMsg = `${debugMsg} ${debugMsg2} DEBUG ${msgDebug} @ ${actualPrice}`;

            const sTradeId = await executeTradeCycleBuy(tc.cConfigId, tc.tConfigId, seq, 'M', actualPrice, actualQty, debugMsg, status, orderId, tc.lastCycleRefCode, tCActivityId);
            logger.info(`Member #${tc.memberId} ${tCActivityId > 0 ? "MANUAL " : ""}OPEN ${symbol} Ref #${sTradeId}`);
            return sTradeId;

        } else {
            // Handle failure in opening the order
            apiError = `Fail to Open Trade for ${symbol}, DCA #${tc.lastCycleRefCode} ${tc.lastCycleSeq === -1 ? "IN" : `CD ${tc.lastCycleSeq + 1}`}`;
            await executeBuyFail(tc.cConfigId, tc.tConfigId, apiError, tCActivityId);
            logger.error(`Member #${tc.memberId} ${tCActivityId > 0 ? "MANUAL " : ""}OPEN FAIL Exchange Status: ${apiError}`);
            logger.error(`Symbol: ${symbol} Order Side: ${openSide} Volume: ${volume} Price: ${lastPrice} Comment: DCA #${tc.lastCycleRefCode} ${tc.lastCycleSeq === -1 ? "IN" : `CD ${tc.lastCycleSeq + 1}`}`);
            return false;
        }
    } catch (error) {
        const errMsg = `Position Open ${tCActivityId > 0 ? '(MANUAL) ' : ''}Error: ${error.message}`;
        await executeBuyFail(tc.cConfigId, tc.tConfigId, errMsg, tCActivityId);
        logger.error(errMsg);
        return false;
    }
};

//Function to Close Position 
async function PositionClose(tc, tCActivityId, symbol, pnlRate, lastPrice, closeSide, msgDebug) {
    
    const orderQty = parseFloat(tc.buyQuantity.toFixed(2));
    const volume = Math.round(orderQty * 100);
    const contractSize = 100000;
    logger.info(`Closing Volume: ${volume}`);
    
    let apiError = '';
    
    try {
        // Close Order
        const ticket = await orderClose(tc.apiKey, closeSide, volume, symbol, tc.orderType_SellOut, lastPrice, `DCA #${tc.lastCycleRefCode} ${tCActivityId > 0 ? 'SELL MANUAL' : tCActivityId == -1 ? 'SL' : 'TP'}`);

        // Handle order close success
        if (ticket) {
            const orderStatus = 'F';
            const orderId = ticket.toString();
            const actualPrice = await getOpenPrice(tc.apiKey, orderId);
            const actualQty =  orderQty;
            const actualCost = Math.floor(actualQty * contractSize);
            const actualEarn = await calculateTotalProfit(tc.apiKey, tc.lastCycleId);
            
            let debugMsg = `${tCActivityId === -1 ? 'CUT LOSS ' : tCActivityId > 0 ? 'MANUAL ' : ''}${closeSide==='B'?'SELL':'BUY'} OUT @ ${(pnlRate * 100).toFixed(4)}% ${actualEarn > 0 ? 'EARN' : 'LOSS'} ${actualEarn.toFixed(2)} `;
            let debugMsg2 = `${symbol}: [Price: ${actualPrice}  Qty: ${actualQty}  Cost: ${actualCost}] `;
            logger.info(debugMsg);
            logger.info(debugMsg2);
            debugMsg = debugMsg + debugMsg2 + `DEBUG ${msgDebug} @ ${actualPrice}`;

            // Save the successful trade to the database
            const sTradeId = await executeTradeCycleSell(tc.cConfigId, tc.tConfigId, 'M', actualPrice, actualQty, debugMsg, orderStatus, orderId, tc.lastCycleRefCode, tCActivityId);
            const pTradeId = await executeTradeCycleProfitUpdate(tc.memberId, tc.lastCycleId, actualEarn);
            logger.info(`Member #${tc.memberId} ${tCActivityId === -1 ? 'CUT LOSS ' : tCActivityId > 0 ? 'MANUAL ' : ''}CLOSE ${symbol} Ref #${sTradeId == pTradeId ? sTradeId : -1}`);
            return sTradeId;
        }
        else {
            // Save failure to the database
            apiError = `Fail to Close Trade for ${symbol}, DCA #${tc.lastCycleRefCode} ${tCActivityId == -1 ? 'SL' : 'TP'}`;
            await executeSellFail(tc.cConfigId, tc.tConfigId, apiError, tCActivityId);
            logger.error(`Member #${tc.memberId} ${tCActivityId === -1 ? 'CUT LOSS ' : tCActivityId > 0 ? 'MANUAL ' : ''}CLOSE FAIL Exchange Status: ${apiError}`);
            logger.error(`Symbol: ${symbol} Order Side: ${closeSide} Volume: ${volume} Price: ${lastPrice} Comment:  DCA #${tc.lastCycleRefCode} ${tCActivityId == -1 ? 'SL' : 'TP'}`);
            return false;
        }
    } catch (error) {
        errMsg = `Position Close ${tCActivityId === -1 ? '(CUT LOSS) ' : tCActivityId > 0 ? '(MANUAL) ' : ''}Error: ${error.message}`;
        await executeSellFail(tc.cConfigId, tc.tConfigId, errMsg, tCActivityId);
        logger.error(errMsg);
        return false;
    }
};

// Function to Monitor and Trade 
async function CFD_Bot_Start(symbol, bSymbolId, bidPrice, askPrice) {

    let tradePerformed = false, tradeCompleted = false;
    
    try {
        // Execute stored procedure to get trade configuration
        const tradeConfig = await executeTradeConfig(symbol, bSymbolId);

        // If no trade config returned, skip further processing
        if (!tradeConfig || tradeConfig.length === 0) return;  

        // Create a Set of API keys from `users` for fast lookup
        const validApiKeys = new Set(users.map(user => user.Login.toString()));

        // Filter trade config to include only API keys present in this server
        const validTradeConfig = tradeConfig.filter(tc => tc.apiKey && validApiKeys.has(tc.apiKey));

        // Process each batch in `validTradeConfig`
        for (let i = 0; i < validTradeConfig.length; i += BATCH_SIZE) {
            await Promise.allSettled(validTradeConfig.slice(i, i + BATCH_SIZE).map(async (tc) => {
                const {
                    orderSide, 
                    coverDownPattern, coverdownMode, coverDownCount, coverDownRate, coverDownQuote, coverPullbackSetup, 
                    profitRate, profitRetraceRate, cutLossRate, 
                    orderQuoteBegin, orderQuoteEnd, 
                    lastPriceLow, lastPriceHigh, lastCycleStatus,
                    commandType, commandArgs, isAllowCoverdown
                } = tc;
    
                let { buyEntryPrice, buyQuantity, buyAvgPrice, lastCycleSeq }= tc;
    
                const lastPriceOpen = orderSide === 'B' ? askPrice : bidPrice;  // Long: Buy at Ask / Short: Sell at Bid
                const lastPriceClose = orderSide === 'B' ? bidPrice : askPrice; // Long: Sell at Bid / Short: Buy at Ask
    
                const coverDownRateCurrent = lastCycleSeq == -1 ? 0 : lastCycleSeq < coverDownCount ? (coverDownRate[lastCycleSeq] / (coverdownMode === 'R' ? 10000 : 1)) : -1;
                const coverDownQuoteCurrent = lastCycleSeq == -1 ? 0 : lastCycleSeq < coverDownCount ? coverDownQuote[lastCycleSeq] : -1;
                const coverPullbackSetupCurrent = lastCycleSeq == -1 ? 0 : lastCycleSeq < coverDownCount ? (coverPullbackSetup[lastCycleSeq] / 10000) : -1;
                let coverDownQuotePriceTriggered = false;
                let orderEarningRate = 0;
                let commandStep = "";      
                
                // Calculate Bounce Rate & Retrace Rate
                let priceLowBounceRate;
                priceLowBounceRate = orderSide === 'B' 
                ? lastPriceLow > 0 ? (lastPriceOpen / lastPriceLow) : lastPriceClose
                : lastPriceHigh > 0 ? (lastPriceHigh / lastPriceOpen) : lastPriceClose;
    
                let priceHighFallRate;
                priceHighFallRate = orderSide === 'B' 
                ? lastPriceHigh > 0 ? (lastPriceClose / lastPriceHigh) : lastPriceClose
                : lastPriceLow > 0 ? (lastPriceLow / lastPriceClose) : lastPriceClose;
    
                try {
                    if (lastCycleStatus === 'F') {  // Fully filled order
                        // Manual Sell
                        if (commandType === 'M' && tc.tCActivityId > 0) {
                            commandStep = "Manual Sell";
    
                            orderEarningRate = orderSide === 'B' 
                            ? buyAvgPrice === 0 ? 0 : (lastPriceClose / buyAvgPrice)
                            : buyAvgPrice === 0 ? 0 : (buyAvgPrice / lastPriceClose);
    
                            const sellDebug = `${(orderEarningRate - 1).toFixed(4)} ${(orderEarningRate - 1) >= profitRate ? `>=` : `<`} ${profitRate.toFixed(4)}`;
                            logger.info(`Member #${tc.memberId} MANUAL CLOSE ${sellDebug}`);
    
                            const tCycleId = await PositionClose(tc, tc.tCActivityId, symbol, (orderEarningRate - 1), lastPriceClose, orderSide, sellDebug);
                            if (tCycleId) tradeCompleted = true;
                        }
    
                        // Manual Buy (K:LONG / L:SHORT)
                        else if ((commandType === 'K' || commandType === 'L') && tc.tCActivityId > 0) {
                            commandStep = `Manual Buy ${commandType === 'K' ? '(LONG)' : '(SHORT)'}`;

                            let manualBuyAmount;
                            let quotePrice;

                            // Validate commandArgs input
                            if (commandArgs && typeof commandArgs === 'string' && commandArgs.trim()) { 
                                // Split commandArgs by '@' to extract both manualBuyAmount and quotePrice if available
                                const argsParts = commandArgs.split('@');

                                // Ensure that at least the first part exists
                                if (argsParts.length > 0) {
                                    manualBuyAmount = parseFloat(argsParts[0]); // First part is manualBuyAmount
                                }

                                // Optional: ensure quotePrice is parsed if it exists
                                if (argsParts.length > 1) {
                                    quotePrice = parseFloat(argsParts[1]); // Second part is quotePrice
                                }

                                // Check for a valid manualBuyAmount
                                if (!isNaN(manualBuyAmount) && manualBuyAmount > 0) {
                                    // Follow Setting, override amount
                                    if (tc.cConfigId > 0) {
                                        manualBuyAmount = Math.floor(manualBuyAmount * tc.clientMultiplier);
                                    }

                                    // Calculate PNL Rate 
                                    orderEarningRate = commandType === 'K'
                                        ? coverDownPattern === 'E' 
                                            ? (buyEntryPrice === 0 ? 0 : lastPriceOpen / buyEntryPrice) 
                                            : (buyAvgPrice === 0 ? 0 : lastPriceOpen / buyAvgPrice)
                                        : coverDownPattern === 'E' 
                                            ? (buyEntryPrice === 0 ? 0 : buyEntryPrice / lastPriceOpen)
                                            : (buyAvgPrice === 0 ? 0 : buyAvgPrice / lastPriceOpen);

                                    const buyDebug = `${(1 - orderEarningRate).toFixed(4)} ${(1 - orderEarningRate) >= coverDownRateCurrent ? `>=` : `<`} ${coverDownRateCurrent.toFixed(4)}`;
                                    logger.info(`Member #${tc.memberId} MANUAL OPEN ${commandType === 'K' ? 'BUY' : 'SELL'} ${manualBuyAmount} ${symbol} @ ${buyDebug}`);

                                    // Use quotePrice if available, otherwise default to lastPriceOpen
                                    const manualBuyPrice = (quotePrice && !isNaN(quotePrice)) ? quotePrice : lastPriceOpen;

                                    const tCycleId = await PositionOpen(tc, tc.tCActivityId, symbol, (1 - orderEarningRate), manualBuyPrice, commandType === 'K' ? 'B' : 'S', buyDebug);
                                    if (tCycleId) {
                                        tradePerformed = true; // Mark trade as performed
                                    } else {
                                        logger.warn(`Failed to open position for ${symbol}.`);
                                        await executeBuyFail(tc.cConfigId, tc.tConfigId, `Failed to open position`, tc.tCActivityId);
                                    }

                                } else {
                                    logger.warn(`Member #${tc.memberId} MANUAL BUY ${symbol} ${(commandType === 'K' ? '(LONG)' : '(SHORT)')} Order: Invalid buy figure ${commandArgs}`);
                                    await executeBuyFail(tc.cConfigId, tc.tConfigId, `Invalid buy figure ${commandArgs}`, tc.tCActivityId);
                                }
                            } else {
                                logger.warn(`Member #${tc.memberId} MANUAL BUY ${symbol} ${(commandType === 'K' ? '(LONG)' : '(SHORT)')} Order: Missing or invalid commandArgs`);
                                await executeBuyFail(tc.cConfigId, tc.tConfigId, `Missing or invalid commandArgs`, tc.tCActivityId);
                            }
                        }

                        // Normal Buy Sell follow setting
                        else {
                            commandStep = "Normal BUY/SELL";
    
                            // Set default values for new entries
                            if (lastCycleSeq == -1) {
                                buyEntryPrice = 0;
                                buyAvgPrice = 0;
                            }
                            
                            // Calculate PNL Rate for Cover Down Pip(s) Mode 
                            if (lastCycleSeq >= 0 && coverdownMode === 'R') {
                                orderEarningRate = orderSide === 'B'
                                ? coverDownPattern === 'E' ? (buyEntryPrice === 0 ? 0 : lastPriceLow / buyEntryPrice) : (buyAvgPrice === 0 ? 0 : lastPriceLow / buyAvgPrice) 
                                : coverDownPattern === 'E' ? (buyEntryPrice === 0 ? 0 : buyEntryPrice / lastPriceHigh) : (buyAvgPrice === 0 ? 0 : buyAvgPrice / lastPriceHigh);
                            }
                            
                            // Handle Cover Down for Quote Price Mode 
                            if (lastCycleSeq >= 0 && coverdownMode === 'Q') {
                                coverDownQuotePriceTriggered = orderSide === 'B'
                                ? lastPriceLow <= coverDownQuoteCurrent ? true : false
                                : lastPriceHigh >= coverDownQuoteCurrent ? true : false
                            }
    
                            // Control Entry Quote in between Quote Config
                            let isQuoteWithinSetting = true;
                            if (orderQuoteBegin > 0 || orderQuoteEnd > 0) {
                                commandStep = "Normal BUY/SELL - Quote Control";
    
                                if (orderQuoteBegin >= orderQuoteEnd) {
                                    // Wrap-around range: check if the price is outside the wrapped range
                                    if (lastPriceOpen < orderQuoteEnd || lastPriceOpen > orderQuoteBegin) {
                                        isQuoteWithinSetting = false;
                                    }
                                } else {
                                    // Normal range: check if the price is outside the defined range
                                    if (lastPriceOpen < orderQuoteBegin || lastPriceOpen > orderQuoteEnd) {
                                        isQuoteWithinSetting = false;
                                    }
                                }
                            }
    
                            // Open Position
                            if (lastCycleSeq === -1 && isQuoteWithinSetting || (isAllowCoverdown && coverDownRateCurrent > 0 && (1 - orderEarningRate) >= coverDownRateCurrent) || coverDownQuotePriceTriggered) {
                                commandStep = "Open Trade Control - Open Control";
    
                                if ((lastCycleSeq === -1) && isQuoteWithinSetting || ((priceLowBounceRate - 1) >= coverPullbackSetupCurrent)) {                   
                                    const buyDebug = lastCycleSeq === -1
                                    ? `${lastPriceOpen}`
                                    : coverdownMode === 'R' 
                                        ? `${(1 - orderEarningRate).toFixed(4)} >= ${coverDownRateCurrent.toFixed(4)} && ${(priceLowBounceRate - 1).toFixed(4)} >= ${(coverPullbackSetupCurrent).toFixed(4)}`
                                        : `${orderSide === 'B' ? `${lastPriceLow} <= ${coverDownQuoteCurrent}` : `${lastPriceHigh} >= ${coverDownQuoteCurrent}`} && ${(priceLowBounceRate - 1).toFixed(4)} >= ${(coverPullbackSetupCurrent).toFixed(4)}`;
    
                                    let pnlRate = orderEarningRate != 0 ? (1 - orderEarningRate) : (orderSide === 'B' ? lastPriceLow : lastPriceHigh);
    
                                    const debugInfo = `Member #${tc.memberId} ${orderSide==='B'?'BUY':'SELL'} ${symbol} OPEN @ ${buyDebug}`;
                                    logger.info(debugInfo);
                                    const tCycleId = await PositionOpen(tc, 0, symbol, pnlRate , lastPriceOpen, orderSide, buyDebug);
                                    if (tCycleId) tradePerformed = true;
                                }
                            } else {  // Close Position
                                if (buyQuantity > 0) {
                                    commandStep = "Normal BUY/SELL - Close Control";
    
                                    // Selling formula use Overall average price
                                    let orderEarningRate = orderSide === 'B' 
                                    ? buyAvgPrice === 0 ? 0 : (lastPriceHigh / buyAvgPrice)
                                    : buyAvgPrice === 0 ? 0 : (buyAvgPrice  / lastPriceLow);
                                    
                                    // Check profit rate and high fall rate conditions
                                    if ((orderEarningRate - 1) >= profitRate) {
                                        if ((1 - priceHighFallRate) >= profitRetraceRate) {
                                            const sellDebug = `${(orderEarningRate - 1).toFixed(4)} >= ${(profitRate).toFixed(4)} && ${(1 - priceHighFallRate).toFixed(4)} >= ${(profitRetraceRate).toFixed(4)}`;
                                            logger.info(`Member #${tc.memberId} ${orderSide === 'B' ? 'BUY' : 'SELL'} ${symbol} CLOSE @ ${sellDebug}`);
                                            const tCycleId = await PositionClose(tc, 0, symbol, (orderEarningRate - 1), lastPriceClose, orderSide, sellDebug);
                                            if (tCycleId) tradeCompleted = true;
                                        }
                                    }
    
                                    // Handle Cut Loss, based on overall earning rate
                                    orderEarningRate = orderSide === 'B' 
                                    ? buyAvgPrice === 0 ? 0 : (lastPriceClose / buyAvgPrice)
                                    : buyAvgPrice === 0 ? 0 : (buyAvgPrice / lastPriceClose);
                                
                                    if ((coverDownRateCurrent === -1 || coverDownQuoteCurrent === -1) && cutLossRate !== 0 && (1-orderEarningRate) >= cutLossRate) {
                                        const sellDebug = `${(1-orderEarningRate).toFixed(4)} >= ${(cutLossRate).toFixed(4)}`;
                                        logger.info(`Member #${tc.memberId} CUT LOSS ${orderSide === 'B' ? 'BUY' : 'SELL'} ${symbol} CLOSE @ ${sellDebug}`);
                                        const tCycleId = await PositionClose(tc, -1, symbol, (1-orderEarningRate), lastPriceClose, orderSide, sellDebug);
                                        if (tCycleId) tradeCompleted = true;
                                    }
                                }
                            }
                        }
                    }
                } catch (ex) {
                    errMsg = `Error in Member #${tc.memberId}, command: ${commandStep}: ${ex.message}`;
                    // Log error into database
                    await executeTradeActivityError(tc.cConfigId, tc.tConfigId, errMsg);
                    logger.error(errMsg);
                }
            }));
        }
    } catch (err) {
        if (err.message.includes('Shutting Down')) {
            await delay(1000);
        } else {
            console.error('Error in CFD_Bot_Start:', err);
        }
        return false;

    } finally {
        return { tradePerformed, tradeCompleted };
    }
};

let firstLogDone = false;  // Global flag to track the first log

// Separate function to handle trading logic asynchronously
async function handleCFD(symbolName, symbolId, bidPrice, askPrice) {
    if (isBusy) return false;
    isBusy = true;

    try {
        const { tradePerformed, tradeCompleted } = await CFD_Bot_Start(symbolName, symbolId, bidPrice, askPrice);

        // First-time logging for both trade cycle and open trades for new API keys
        if (!firstLogDone) {
            await logTradingData('A');  // Log active trades first time
            firstLogDone = true;
        }

        // Check for trade events and log accordingly
        if (tradePerformed || tradeCompleted) {
            if (tradePerformed) {
                await logTradingData('A');  // Log active trade cycles on new trade
                logger.info('New trade performed.');
            }
            if (tradeCompleted) {
                await logTradingData('P');  // Log completed trade cycles on trade completion
                logger.info('Trade completed.');
                await delay(10000);  // 10-second delay after trade completion
            }
        }

    } catch (error) {
        console.error('Error in handleCFD:', error);
        return false;
    } finally {
        isBusy = false;
    }

    return true; // Indicate completion
};

// Listen for both SIGINT and SIGTERM signals
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

// Main function
const main = async () => {
    try {
        // Initialize Database Connection
        await initializeDBConnection();

        // Get All Users from Server and store in global `users`
        users = await getUserList();
        
        // Start the updateUserList function to update user list every 30 minutes
        updateUserList(); // No await here, this runs in the background

        // Start the updateUserAccountBalances loop in the background
        updateUserAccountBalances(); // No await here, this runs in the background

        // Start the SignalR client with an async callback for handling updates
        startSignalRClient(async (symbolName, symbolId, bidPrice, askPrice) => {
            try {
                // Save the price into the database
                await executeSymbolPriceUpdate(symbolName, symbolId, bidPrice, askPrice);
                
                // Handle trading logic
                await handleCFD(symbolName, symbolId, bidPrice, askPrice);
                
            } catch (err) {
                if (err.message.includes('Shutting Down')) {
                    await delay(1000); // Wait and try again if shutting down
                } else {
                    console.error('Error in trading loop:', err); // Log error for debugging
                }
            }
        });

        // Schedule the swap fee update to run daily at 00:00 UTC+3:30 (03:30 UTC)
        scheduleSwapFeeUpdate();

    } catch (err) {
        console.error('Error in main function:', err); // Handle errors in the main function
    }
};

// Invoke the main function
main().catch(err => console.error('Unhandled error:', err));
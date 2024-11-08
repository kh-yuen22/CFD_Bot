const axios = require('axios');
const config = require(`./config/config_server1.js`);
const logger = require('./utils/logger');

const API_URL = config.api.URL;

/**
 * Enhanced retryWithBackoff function
 * 
 * @param {Function} fn - The function to execute.
 * @param {Array} params - Parameters to pass to the function.
 * @param {String} commandStep - Description of the operation.
 * @param {Number} retries - Number of retry attempts. Pass `Infinity` to retry indefinitely.
 * @param {Number} delayMs - Initial delay in milliseconds.
 * @returns {Promise<any>}
 */
async function retryWithBackoff(fn, params = [], commandStep = 'API Call', retries = 3, delayMs = 3000) {
    let attempt = 0;
    let hasRetried = false;  // Track if any retries have occurred

    while (attempt < retries || retries === Infinity) {
        try {
            const result = await fn(...params); // Call the API function with provided parameters

            if (hasRetried) {
                // Log 'Retry successful' if retries were attempted
                logger.info(`Retry successful for '${commandStep}' after ${attempt} attempts. Params: ${JSON.stringify(params)}`);
            }

            return result; // Return the result of the successful API call
        } catch (err) {
            const isServerError = err.response && err.response.status === 500;

            // If the error is a server internal error, ignore retry limit (retry indefinitely)
            if (isServerError) {
                retries = Infinity;  // Override to allow unlimited retries
            } else {
                attempt++;
                if (attempt >= retries) {
                    logger.error(`Failed '${commandStep}' after ${attempt} attempts. Error: ${err.message}`, { error: err.response?.data, params });
                    throw err;  // If max retries exceeded, throw error
                }
            }

            hasRetried = true;  // Mark that we are retrying
            const backoff = delayMs * Math.pow(2, attempt);  // Exponential backoff
            logger.warn(`Retrying '${commandStep}' after ${backoff}ms... (Attempt ${attempt}/${retries === Infinity ? "∞" : retries})`, { error: err.message, params });
            await new Promise(resolve => setTimeout(resolve, backoff));  // Wait before retrying
        }
    }
};

/**
 * Fetches the user list from Server 1 with retry and backoff.
 *
 * @returns {Promise<Array>} - Resolves with the list of users if successful, or an empty array if the request fails.
 */
async function getUserList() {
    return await retryWithBackoff(async () => {
        // Send a GET request to fetch the user list from this Server 
        const serverResponse = await axios.get(`${API_URL}/User/List`);
        const serverData = serverResponse.data;

        // Validate the server response to ensure we have user data
        if (!Array.isArray(serverData)) {
            throw new Error("Unexpected data format: Expected an array of users");
        }

        // Log the retrieved user data for debugging purposes
        //console.log("Fetched user list from Server 1:", serverData);

        // Return the user data
        return serverData;

    }, [], `Get User List for this Server`);
};

/**
 * Fetches user account data from the API.
 * 
 * @param {Array} users - List of user objects.
 * @returns {Promise<Array>}
 */
async function getUserData(users) {
    return await retryWithBackoff(async () => {
        const apiResponses = await Promise.all(users.map(async (user) => {
            try {
                // Make the request to the correct server
                const response = await retryWithBackoff(async () => {
                    return await axios.get(`${API_URL}/User/Get`, { params: { Logins: user.apiKey } });
                }, [], `Get User Data for Member ID ${user.memberId}`);

                if (response.status === 200) {
                    const data = response.data;

                    // Validate the response structure
                    if (!data || typeof data.Balance === 'undefined') {
                        logger.error(`Unexpected response structure for Member ID ${user.memberId}: ${JSON.stringify(data)}`);
                        throw new Error('Unexpected response structure');
                    }                    

                    return {
                        memberId: user.memberId,
                        bExchangeId: user.bExchangeId,
                        tradeApiType: user.tradeApiType,
                        login: data.Login,
                        name: data.Name,
                        balance: data.Balance || 0, // Default to 0 if Balance is not present
                        apiSecret: user.apiSecret // Include apiSecret for comparison
                    };
                } else {
                    throw new Error('Unexpected response status');
                }

            } catch (error) {
                const errorMsg = error.message;
                logger.error(`Error getting user account balance for Member ID ${user.memberId}: ${errorMsg}`, { user });
                return { 
                    login: user.apiKey,
                    error: errorMsg,
                    apiSecret: user.apiSecret // Include apiSecret for error handling
                };
            }
        }));

        return apiResponses;
    }, [users], `Get User Account Data`);
};

/**
 * Retrieves open trades for a specific API key.
 * 
 * @param {String} apiKey - The API key.
 * @returns {Promise<Array>}
 */
async function getOpenTrades(apiKey) {
    return await retryWithBackoff(async () => {
        const response = await axios.get(`${API_URL}/Trade/list`, { params: { logins: apiKey, displayOpenTradesOnly: true } });

        // Validate the response
        if (response.status === 200) {
            const tradeData = response.data;

            if (Array.isArray(tradeData)) {
                const trades = tradeData.map(trade => ({
                    Symbol: trade.Symbol ?? 'Unknown',
                    OrderId: trade.Order ?? 'N/A',
                    OpenPrice: trade.OpenPrice ?? 0,
                    Volume: trade.Volume ?? 0,
                    Profit: trade.Profit ?? 0,
                    Swap: trade.Storage ?? 0,
                    Comment: trade.Comment ?? ''
                }));

                return trades;
            } else {
                logger.error(`Invalid trade data structure for API Key ${apiKey}: ${JSON.stringify(tradeData)}`);
                return [];
            }
        } else {
            logger.error(`Failed to fetch open trades for API Key ${apiKey}. Status: ${response.status}`, { response: response.data });
            return [];
        }
    }, [apiKey], `Get Open Trades for API Key ${apiKey}`);
};

/**
 * Checks if a symbol does not exist in open trades for a given API key.
 * 
 * @param {String} apiKey - The API key.
 * @param {String} symbol - The symbol to check.
 * @returns {Promise<Boolean>}
 */
async function isSymbolNotInOpenTrades(apiKey, symbol) {
    const openTrades = await getOpenTrades(apiKey); // Fetch open trades

    // Check if the symbol exists in any of the open trades
    const symbolExists = openTrades.some(trade => trade.Symbol === symbol);

    return !symbolExists; // Return true if symbol does not exist, false if it does
};

/**
 * Opens a trade for a user.
 * 
 * @param {String} loginId - User's login ID.
 * @param {String} openSide - 'B' for Buy or 'S' for Sell.
 * @param {Number} volume - Trade volume.
 * @param {String} symbol - Trading symbol.
 * @param {String} orderType_BuyIn - 'M' for Market or other types.
 * @param {Number} lastPrice - Last traded price.
 * @param {String} comment - Optional comment.
 * @returns {Promise<String|null>} - Returns the trade order ID or null.
 */
async function orderOpen(loginId, openSide, volume, symbol, orderType_BuyIn, lastPrice, comment) {
    // Validate the inputs
    if (!symbol || !volume || volume <= 0) {
        logger.error(`Invalid input for orderOpen: symbol='${symbol}', volume=${volume}`);
        return null;
    }

    return await retryWithBackoff(async () => {
        logger.info(`Placing order open for Login ID: ${loginId}, Symbol: ${symbol}, Volume: ${volume}`);
        const tradePayload = {
            "Login": loginId,
            "Command": openSide === 'B' ? 0 : 1,  // 0 for Buy, 1 for Sell
            "Volume": volume,
            "Symbol": symbol,
            "Price": orderType_BuyIn === 'M' ? 0 : lastPrice,  // 0 for market price, otherwise lastPrice
            "Comment": comment
        };

        const response = await axios.post(`${API_URL}/Trade/Open`, tradePayload);

        // Check if response contains valid data
        if (response.status === 200 && response.data && response.data.Order) {
            logger.info(`Order opened successfully. Order ID: ${response.data.Order}`);
            return response.data.Order;  // Return the trade ticket/order
        } else {
            logger.error(`Unexpected trade response for Login ID ${loginId}: ${JSON.stringify(response.data)}`);
            return null;
        }
    }, [loginId, openSide, volume, symbol, orderType_BuyIn, lastPrice, comment], `Order Open for LoginId ${loginId}`);
};

/**
 * Closes all trades for a user by opening a hedge trade and then attempting to close all trades for the symbol with retries.
 * 
 * @param {String} loginId - User's login ID.
 * @param {String} openSide - 'B' for Buy or 'S' for Sell.
 * @param {Number} volume - Trade volume.
 * @param {String} symbol - Trading symbol.
 * @param {String} orderType_SellOut - 'M' for Market or other types.
 * @param {Number} lastPrice - Last traded price.
 * @param {String} comment - Optional comment.
 * @param {Number} maxRetries - Maximum number of retries for closing trades.
 * @returns {Promise<String>} - Returns the hedge trade order ID if successful.
 */
async function orderClose(loginId, openSide, volume, symbol, orderType_SellOut, lastPrice, comment, maxRetries = 5) {
    let commandStep = `Order Close for LoginId ${loginId} - Open Hedge`;

    try {
        logger.info(`Starting order close process for Login ID: ${loginId}, Symbol: ${symbol}`);

        // Step 1: Open Hedge Trade
        const openHedgePayload = {
            "Login": loginId,
            "Command": openSide === 'B' ? 1 : 0,
            "Volume": volume,
            "Symbol": symbol,
            "Price": orderType_SellOut === 'M' ? 0 : lastPrice,
            "Comment": comment
        };

        const hedgeResponse = await retryWithBackoff(async () => {
            logger.info(`Opening hedge trade for Login ID: ${loginId}, Symbol: ${symbol}`);
            return await axios.post(`${API_URL}/Trade/Open`, openHedgePayload);
        }, [loginId, openSide, volume, symbol, orderType_SellOut, lastPrice, comment], commandStep);

        // Verify hedge trade success
        if (!hedgeResponse.data || !hedgeResponse.data.Order) {
            throw new Error(`Failed to open hedge trade, unexpected response: ${JSON.stringify(hedgeResponse.data)}`);
        }

        const hedgeOrderId = hedgeResponse.data.Order;
        logger.info(`Hedge trade opened successfully. Order ID: ${hedgeOrderId}`);
        commandStep = `Order Close for LoginId ${loginId} - Close All`;

        // Step 2: Close All Trades with retries and backoff
        let retries = 0;
        while (retries < maxRetries) {
            const closeAllPayload = { "Login": loginId, "Symbol": symbol };
            const closeResponse = await retryWithBackoff(async () => {
                logger.info(`Closing all trades for Login ID: ${loginId}, Symbol: ${symbol}, Attempt: ${retries + 1}`);
                return await axios.post(`${API_URL}/Trade/CloseAll`, closeAllPayload);
            }, [loginId, symbol], commandStep);

            logger.info(`Close all response: ${JSON.stringify(closeResponse.data)}`);

            // Verify if trades are closed by checking open positions
            const isNotInTrades = await isSymbolNotInOpenTrades(loginId, symbol);
            if (isNotInTrades) {
                logger.info(`All trades closed successfully for Login ID: ${loginId}, Symbol: ${symbol}`);
                return hedgeOrderId; // Successfully closed all trades
            }

            // If trades remain, log and prepare for retry
            logger.warn(`Trades still open for Symbol: ${symbol}. Retry ${retries + 1}/${maxRetries}`);
            retries++;

            // Exponential backoff delay
            const delayMs = 1000 * Math.pow(2, retries); // E.g., 1s, 2s, 4s, etc.
            await new Promise(resolve => setTimeout(resolve, delayMs));
        }

        // Final check after max retries
        const isNotInTradesFinal = await isSymbolNotInOpenTrades(loginId, symbol);
        if (isNotInTradesFinal) {
            logger.info(`All trades closed after retries for Login ID: ${loginId}, Symbol: ${symbol}`);
            return hedgeOrderId;
        } else {
            logger.error(`Failed to close all trades for symbol '${symbol}' after ${maxRetries} retries.`);
            throw new Error(`Partial failure: could not close all trades for symbol '${symbol}' after retries.`);
        }

    } catch (error) {
        logger.error(`Error in '${commandStep}' for Login ID: ${loginId}, Symbol: ${symbol}: ${error.message}`, { error });
        throw error;
    }
};


/**
 * Retrieves active trade data for a specific order.
 * 
 * @param {String} apiKey - The API key.
 * @param {String} orderId - The order ID.
 * @returns {Promise<Array|null>}
 */
async function getTradeData(apiKey, orderId) {
    return await retryWithBackoff(async () => {
        const response = await axios.get(`${API_URL}/Trade/Get`, { params: { orders: orderId } });

        if (response.status === 200) {
            const tradeData = response.data;

            if (Array.isArray(tradeData) && tradeData.length > 0) {
                return tradeData;  // Return full trade data for the order
            } else {
                logger.warn(`No trade data returned for Order ID: ${orderId}.`);
                return null;  // Explicitly return null if no data found
            }
        } else {
            logger.error(`API call failed for Order ID: ${orderId}. Status: ${response.status}`, { response: response.data });
            return null;  // Return null in case of an API failure
        }
    }, [apiKey, orderId], `Get Trade Data for Order ID: ${orderId}, API Key: ${apiKey}`);
};

module.exports = { getUserList, getUserData, getOpenTrades, orderOpen, orderClose, getTradeData };

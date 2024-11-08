// utils/validateApiKey.js

const axios = require('axios');
const logger = require('./logger');  // Import logger if available

// URL constants for both servers
const SERVER1_URL = process.env.API_SERVER1_URL;
const SERVER2_URL = process.env.API_SERVER2_URL;

if (!SERVER1_URL || !SERVER2_URL) {
    throw new Error("API server URLs are not defined in environment variables.");
}

/**
 * Validates an API key against two servers with retry and backoff.
 * 
 * @param {String} apiKey - The API key to validate.
 * @param {Number} retries - Number of retry attempts. Use Infinity for unlimited retries.
 * @param {Number} delayMs - Initial delay in milliseconds for exponential backoff.
 * @returns {Promise<{ valid: Boolean, server: String|null }>}.
 */
async function validateApiKey(apiKey, retries = 3, delayMs = 3000) {
    let attempt = 0;

    while (attempt < retries || retries === Infinity) {
        try {
            // Make request to SERVER1
            const server1Response = await axios.get(`${SERVER1_URL}/User/List`);
            if (server1Response.data.some(user => user.Login == apiKey)) {
                if (attempt > 0) {
                    logger.info(`Retry successful for API key '${apiKey}' on SERVER1 after ${attempt} attempt(s).`);
                }
                return { valid: true, server: 'SERVER1' };
            }

            // Make request to SERVER2
            const server2Response = await axios.get(`${SERVER2_URL}/User/List`);
            if (server2Response.data.some(user => user.Login == apiKey)) {
                if (attempt > 0) {
                    logger.info(`Retry successful for API key '${apiKey}' on SERVER2 after ${attempt} attempt(s).`);
                }
                return { valid: true, server: 'SERVER2' };
            }

            // If neither server returns the key, return valid: false
            return { valid: false, server: null };

        } catch (err) {
            const statusCode = err.response?.status;
            const isServerError = statusCode === 500; // Internal Server error 

            if (isServerError) {
                retries = Infinity; // Retry indefinitely if server error
            } else {
                attempt++;
                if (attempt >= retries) {
                    logger.error(`Validation failed for API key '${apiKey}' after ${attempt} attempts. Error: ${err.message}`, {
                        statusCode,
                        response: err.response?.data,
                    });
                    throw new Error(`API key '${apiKey}' validation failed: ${err.message}`);
                }
            }

            const backoff = delayMs * Math.pow(2, attempt);
            logger.warn(`Retrying validation for API key '${apiKey}' after ${backoff}ms (Attempt ${attempt}/${retries === Infinity ? "∞" : retries})`);
            await new Promise(resolve => setTimeout(resolve, backoff));
        }
    }
}


module.exports = { validateApiKey };

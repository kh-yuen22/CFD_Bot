const winston = require('winston');
const DailyRotateFile = require('winston-daily-rotate-file');
const path = require('path');
const fs = require('fs');

// Helper to get the full or abbreviated month name
function getMonthName(monthIndex, full = false) {
    const months = full
        ? ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
        : ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    return months[monthIndex];
};

// Function to get the path with the current year and month
function getLogDirectory() {
    const date = new Date();
    const year = date.getFullYear();
    const month = getMonthName(date.getMonth(), true);
    const logDir = path.join(__dirname, 'logs', year.toString(), month);

    // Create directories if they don't exist
    fs.mkdirSync(logDir, { recursive: true });
    return logDir;
};

// Custom log format
const customFormat = winston.format.printf(({ timestamp, level, message }) => {
    return `[${timestamp}] ${level.toUpperCase()}: ${message}`;
});

// Create a logger with daily file rotation
const transports = [
    new winston.transports.Console({
        format: winston.format.combine(
            winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss.SS' }),
            customFormat
        )
    }),
    new DailyRotateFile({
        filename: path.join(getLogDirectory(), '%DATE%.log'),
        datePattern: 'YYYY-MM-DD',
        zippedArchive: false,
        maxSize: '20m',
        maxFiles: '365d',
        level: 'info',
        format: winston.format.combine(
            winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss.SS' }),
            customFormat
        )
    }),
    new winston.transports.File({
        filename: './utils/logs/error.log',
        level: 'error',
        format: winston.format.combine(
            winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss.SS' }),
            customFormat
        )
    })
];

// Initialize logger
const logger = winston.createLogger({
    level: 'info',
    transports
});

// Helper function for file-only logs
logger.file = (message) => {
    // Temporarily disable the console for this log
    transports[0].silent = true;
    logger.info(message);
    transports[0].silent = false; // Re-enable console after logging
};

module.exports = logger;

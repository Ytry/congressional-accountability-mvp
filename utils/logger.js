const { createLogger, format, transports } = require('winston');
const DailyRotateFile = require('winston-daily-rotate-file');

// Destructure format helpers
const { combine, timestamp, json, errors, printf, colorize } = format;

// Environment-based log level and metadata
const level = process.env.LOG_LEVEL || 'info';
const environment = process.env.NODE_ENV || 'development';

// Define the JSON log format with timestamp and error stack
const jsonFormat = combine(
  timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
  errors({ stack: true }),
  json()
);

// Define a console format for development readability
const consoleFormat = combine(
  timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
  errors({ stack: true }),
  colorize(),
  printf(({ timestamp, level, message, ...metadata }) => {
    const meta = Object.keys(metadata).length ? JSON.stringify(metadata) : '';
    return `${timestamp} [${level}]: ${message} ${meta}`;
  })
);

// Create the Winston logger instance
const logger = createLogger({
  level,
  defaultMeta: { service: 'backend', environment },
  format: jsonFormat,
  transports: [
    // Console transport for development
    new transports.Console({
      format: consoleFormat
    }),

    // Daily rotating file transport for production
    new DailyRotateFile({
      filename: 'logs/app-%DATE%.log',
      datePattern: 'YYYY-MM-DD',
      zippedArchive: true,
      maxSize: '20m',
      maxFiles: '14d',
      format: jsonFormat
    })
  ],
  exitOnError: false,
});

// Handle uncaught exceptions and unhandled promise rejections
logger.exceptions.handle(
  new DailyRotateFile({
    filename: 'logs/exceptions-%DATE%.log',
    datePattern: 'YYYY-MM-DD',
    zippedArchive: true,
    maxFiles: '14d',
    format: jsonFormat
  })
);

logger.rejections.handle(
  new DailyRotateFile({
    filename: 'logs/rejections-%DATE%.log',
    datePattern: 'YYYY-MM-DD',
    zippedArchive: true,
    maxFiles: '14d',
    format: jsonFormat
  })
);

// Optional stream for morgan or other middlewares
logger.stream = {
  write: (message) => {
    // Remove trailing newline
    logger.info(message.trim());
  },
};

module.exports = logger;

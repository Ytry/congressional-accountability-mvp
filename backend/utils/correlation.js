const { v4: uuidv4 } = require('uuid');
const logger = require('./logger');

/**
 * Express middleware to attach a correlation ID to each request,
 * expose it in response headers, and provide a child-logger scoped to that ID.
 */
module.exports = function correlationId(req, res, next) {
  // Use incoming header if provided, otherwise generate a new UUID
  const incoming = req.header('X-Correlation-ID');
  const correlationId = incoming && incoming.trim() ? incoming.trim() : uuidv4();

  // Attach to request and response
  req.correlationId = correlationId;
  res.setHeader('X-Correlation-ID', correlationId);

  // Create a child logger with correlationId and request context
  req.logger = logger.child({
    correlationId,
    method: req.method,
    url: req.originalUrl
  });

  // Timestamp for duration metrics
  req.startTime = Date.now();

  // Log request receipt
  req.logger.info('Request received');

  // After response is sent, log completion details
  res.on('finish', () => {
    const duration = Date.now() - req.startTime;
    req.logger.info('Request completed', {
      statusCode: res.statusCode,
      durationMs: duration
    });
  });

  next();
};

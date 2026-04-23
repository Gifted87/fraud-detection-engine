import pino from 'pino';

/**
 * Standardized High-Performance Logger (Pino).
 * 
 * Optimized for high-throughput financial environments by using 
 * non-blocking asynchronous logging and structured JSON format. 
 * Prevents event loop blocking caused by synchronous console.log calls.
 */
export const logger = pino({
  level: process.env.LOG_LEVEL || 'info',
  base: {
    service: 'fraud-detection-engine',
    env: process.env.NODE_ENV || 'production',
  },
  timestamp: pino.stdTimeFunctions.isoTime,
  formatters: {
    level: (label) => {
      return { level: label.toUpperCase() };
    },
  },
});

/**
 * Standard context wrapper for structured logging.
 */
export function logContext(context: Record<string, unknown>) {
  return logger.child(context);
}

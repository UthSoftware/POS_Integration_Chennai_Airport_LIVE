const Queue = require('bull');
const logger = require('../config/logger');

class VendorQueue {
  constructor() {
    this.queue = new Queue('vendor-data-processing', {
      redis: {
        host: process.env.REDIS_HOST || 'localhost',
        port: parseInt(process.env.REDIS_PORT) || 6379,
        password: process.env.REDIS_PASSWORD || undefined
      },
      defaultJobOptions: {
        attempts: parseInt(process.env.QUEUE_MAX_RETRIES) || 3,
        backoff: {
          type: 'exponential',
          delay: 2000
        },
        removeOnComplete: 100,
        removeOnFail: 500
      }
    });

    this.setupEventHandlers();
  }

  setupEventHandlers() {
    this.queue.on('completed', (job, result) => {
      logger.info('Job completed', {
        jobId: job.id,
        duration: Date.now() - job.timestamp,
        recordsProcessed: result.recordsProcessed
      });
    });

    this.queue.on('failed', (job, err) => {
      logger.error('Job failed', {
        jobId: job.id,
        error: err.message,
        attempts: job.attemptsMade,
        vendorId: job.data.metadata?.vendorId
      });
    });

    this.queue.on('error', (error) => {
      logger.error('Queue error', { error: error.message });
    });

    this.queue.on('stalled', (job) => {
      logger.warn('Job stalled', { jobId: job.id });
    });
  }

  async addJob(data, metadata, priority = 10) {
    try {
      const job = await this.queue.add({
        data,
        metadata: {
          ...metadata,
          receivedAt: new Date().toISOString()
        }
      }, {
        priority,
        jobId: `vendor-${metadata.vendorId || 'unknown'}-${Date.now()}`
      });

      logger.info('Job added to queue', {
        jobId: job.id,
        recordCount: Array.isArray(data) ? data.length : 1
      });

      return job;
    } catch (error) {
      logger.error('Error adding job to queue', { error: error.message });
      throw error;
    }
  }

  async getJobStatus(jobId) {
    const job = await this.queue.getJob(jobId);
    if (!job) return null;

    const state = await job.getState();
    return {
      id: job.id,
      state,
      progress: job.progress(),
      data: job.data,
      result: job.returnvalue,
      failedReason: job.failedReason,
      attemptsMade: job.attemptsMade
    };
  }

  async getQueueStats() {
    const [waiting, active, completed, failed, delayed] = await Promise.all([
      this.queue.getWaitingCount(),
      this.queue.getActiveCount(),
      this.queue.getCompletedCount(),
      this.queue.getFailedCount(),
      this.queue.getDelayedCount()
    ]);

    return { waiting, active, completed, failed, delayed };
  }

  async close() {
    await this.queue.close();
    logger.info('Queue closed');
  }
}

module.exports = new VendorQueue();
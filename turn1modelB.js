// ownership-manager.js
const redis = require('ioredis');

const redisClient = new redis("rediss://default:AVNS_wTdZwTrFwgPndeKx_E9@caching-26367da2-kimpact.k.aivencloud.com:13890");

const LOCK_EXPIRY = 10000; // 10 seconds

class OwnershipManager {
    async acquireLock(resource) {
        const lockKey = `lock:${resource}`;
        const now = Date.now();

        while (true) {
            const result = await redisClient.set(lockKey, now, 'PX', LOCK_EXPIRY, 'NX');
            if (result === 'OK') {
                // Successfully acquired the lock
                return true;
            }

            // Check if the lock has expired
            const expiration = await redisClient.ttl(lockKey);
            if (expiration === -2) {
                // The lock doesn't exist, so try to acquire it again
                continue;
            }

            // The lock is still held by another node, so wait and retry
            await new Promise((resolve) => setTimeout(resolve, 100));
        }
    }

    async releaseLock(resource) {
        const lockKey = `lock:${resource}`;
        await redisClient.del(lockKey);
    }

    async isOwned(resource) {
        const lockKey = `lock:${resource}`;
        const expiration = await redisClient.ttl(lockKey);
        return expiration !== -2;
    }
}

// module.exports = OwnershipManager;

// index.js
// const OwnershipManager = require('./ownership-manager');

const om = new OwnershipManager();

async function updateEnergyConsumption(stationId) {
    try {
        // Acquire the lock for the specific station's data
        const acquiredLock = await om.acquireLock(stationId);
        if (!acquiredLock) {
            console.log(`Failed to acquire lock for station ${stationId}.`);
            return;
        }

        console.log(`Acquired lock for station ${stationId}.`);

        // Simulate processing the data and updating the shared resource
        await new Promise((resolve) => setTimeout(resolve, 2000)); // 2 seconds

        console.log(`Releasing lock for station ${stationId}.`);
        await om.releaseLock(stationId);
    } catch (error) {
        console.error(`Error processing station ${stationId}: ${error.message}`);
    }
}

// Example usage:
updateEnergyConsumption('station1');
updateEnergyConsumption('station2');
updateEnergyConsumption('station3');

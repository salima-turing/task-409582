// ownership-manager.js
const redis = require('ioredis');

const redisClient = new redis("rediss://default:AVNS_wTdZwTrFwgPndeKx_E9@caching-26367da2-kimpact.k.aivencloud.com:13890");

const LOCK_EXPIRY = 10000; // 10 seconds
const HEARTBEAT_INTERVAL = 3000; // 3 seconds

class OwnershipManager {
    async acquireLock(resource, ownerId) {
        const lockKey = `lock:${resource}`;
        const now = Date.now();

        const result = await redisClient.set(lockKey, JSON.stringify({
            ownerId,
            timestamp: now,
        }), 'PX', LOCK_EXPIRY, 'NX');

        if (result === 'OK') {
            // Successfully acquired the lock
            return true;
        }

        // Lock already exists; check ownership and steal lock if necessary
        const lockValue = await redisClient.get(lockKey);
        const lockData = JSON.parse(lockValue);

        if (lockData && (now - lockData.timestamp > LOCK_EXPIRY)) {
            // If the lock is expired, attempt to steal
            await redisClient.set(lockKey, JSON.stringify({
                ownerId,
                timestamp: now,
            }), 'PX', LOCK_EXPIRY); // Overwrite the existing value
            return true; // Lock stolen
        }

        // Lock is currently held by another active owner
        return false;
    }

    async releaseLock(resource, ownerId) {
        const lockKey = `lock:${resource}`;
        const lockValue = await redisClient.get(lockKey);
        const lockData = JSON.parse(lockValue);

        // Only release the lock if the current owner matches
        if (lockData && lockData.ownerId === ownerId) {
            await redisClient.del(lockKey);
        }
    }

    async sendHeartbeat(resource, ownerId) {
        const lockKey = `lock:${resource}`;
        const now = Date.now();
        const lockValue = await redisClient.get(lockKey);
        const lockData = JSON.parse(lockValue);

        if (lockData && lockData.ownerId === ownerId) {
            // Update the timestamp to extend ownership
            await redisClient.set(lockKey, JSON.stringify({
                ...lockData,
                timestamp: now,
            }), 'PX', LOCK_EXPIRY);
        }
    }
}

// module.exports = OwnershipManager;

// index.js
// const OwnershipManager = require('./ownership-manager');

const om = new OwnershipManager();
const ownerId = `node-${process.pid}`; // Unique ID for the node

async function updateEnergyConsumption(stationId) {
    const lockAcquired = await om.acquireLock(stationId, ownerId);

    if (!lockAcquired) {
        console.log(`Failed to acquire lock for station ${stationId}.`);
        return;
    }

    console.log(`Acquired lock for station ${stationId}.`);

    // Start sending heartbeats
    const heartbeatInterval = setInterval(async () => {
        await om.sendHeartbeat(stationId, ownerId);
    }, HEARTBEAT_INTERVAL);

    try {
        // Simulate processing the data and updating the shared resource
        await new Promise((resolve) => setTimeout(resolve, 5000)); // 5 seconds
    } catch (error) {
        console.error(`Error processing station ${stationId}: ${error.message}`);
    } finally {
        console.log(`Releasing lock for station ${stationId}.`);
        await om.releaseLock(stationId, ownerId);
        clearInterval(heartbeatInterval); // Stop sending heartbeats
    }
}

// Example usage:
updateEnergyConsumption('station1');
updateEnergyConsumption('station2');
updateEnergyConsumption('station3');

// ownership-manager.js
const redis = require('ioredis');

const redisClient = new redis("rediss://default:AVNS_wTdZwTrFwgPndeKx_E9@caching-26367da2-kimpact.k.aivencloud.com:13890");

const LOCK_EXPIRY = 10000; // 10 seconds
const HEALTH_CHECK_INTERVAL = 5000; // 5 seconds

class OwnershipManager {
    constructor() {
        this.ownsLocks = new Map();
    }

    async acquireLock(resource) {
        const lockKey = `lock:${resource}`;
        const now = Date.now();

        while (true) {
            const result = await redisClient.set(lockKey, now, 'PX', LOCK_EXPIRY, 'NX');
            if (result === 'OK') {
                // Successfully acquired the lock
                this.ownsLocks.set(resource, true);
                this.startHealthCheck(resource);
                return true;
            }

            // Check if the lock has expired (another node could have grabbed it)
            const expiration = await redisClient.ttl(lockKey);
            if (expiration === -2) {
                // The lock doesn't exist, so try to acquire it again
                continue;
            }

            // The lock is still held by another node, so check for health
            const holder = await redisClient.get(lockKey);
            if (holder === null) {
                // The lock value has expired, so try to steal it
                if (await this.stealLock(resource, now)) {
                    return true;
                }
            }

            // Wait and retry
            await new Promise((resolve) => setTimeout(resolve, 100));
        }
    }

    async releaseLock(resource) {
        const lockKey = `lock:${resource}`;
        if (this.ownsLocks.has(resource)) {
            await redisClient.del(lockKey);
            this.ownsLocks.delete(resource);
            clearInterval(this.healthChecks.get(resource));
            this.healthChecks.delete(resource);
        }
    }

    async stealLock(resource, now) {
        const lockKey = `lock:${resource}`;
        const lockValue = await redisClient.getset(lockKey, now);

        if (lockValue === null) {
            // The lock has expired or was stolen already, try again later
            return false;
        }

        const victim = Number(lockValue);
        const msToVictim = now - victim;

        if (msToVictim > LOCK_EXPIRY) {
            // The victim is unresponsive, assume it crashed and steal the lock
            console.log(`Stealing lock for ${resource} from victim: ${victim}`);
            return true;
        } else {
            console.log(`Victim ${victim} is alive, ignoring lock steal attempt for ${resource}.`);
        }
        return false;
    }

    startHealthCheck(resource) {
        this.healthChecks = new Map();

        const healthCheck = setInterval(async () => {
            const lockKey = `lock:${resource}`;
            const now = Date.now();
            const holder = await redisClient.get(lockKey);

            if (holder === null) {
                // The lock has expired, assume it was stolen by another node.
                console.log(`Lost lock for ${resource}.`);
                this.ownsLocks.delete(resource);
                clearInterval(healthCheck);
                return;
            }

            const msToHolder = now - Number(holder);
            if (msToHolder > LOCK_EXPIRY) {
// The holder is unresponsive, try to steal the lock

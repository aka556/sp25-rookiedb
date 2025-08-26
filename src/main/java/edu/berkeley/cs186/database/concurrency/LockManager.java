package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have what locks
 * on what resources and handles queuing logic. The lock manager should generally
 * NOT be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with multiple
 * levels of granularity. Multigranularity is handled by LockContext instead.
 *
 * Each resource the lock manager manages has its own queue of LockRequest
 * objects representing a request to acquire (or promote/acquire-and-release) a
 * lock that could not be satisfied at the time. This queue should be processed
 * every time a lock on that resource gets released, starting from the first
 * request, and going in order until a request cannot be satisfied. Requests
 * taken off the queue should be treated as if that transaction had made the
 * request right after the resource was released in absence of a queue (i.e.
 * removing a request by T1 to acquire X(db) should be treated as if T1 had just
 * requested X(db) and there were no queue on db: T1 should be given the X lock
 * on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is
 * processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();

    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // Below are a list of helper methods we suggest you implement.
        // You're free to modify their type signatures, delete, or ignore them.

        /**
         * Check if `lockType` is compatible with preexisting locks. Allows
         * conflicts for locks held by transaction with id `except`, which is
         * useful when a transaction tries to replace a lock it already has on
         * the resource.
         */
        public boolean checkCompatible(LockType lockType, long except) {
            // TODO(proj4_part1): implement
            for (Lock l : locks) {
                if (l.transactionNum == except) continue;

                if (!LockType.compatible(l.lockType, lockType)) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Gives the transaction the lock `lock`. Assumes that the lock is
         * compatible. Updates lock on resource if the transaction already has a
         * lock.
         */
        public void grantOrUpdateLock(Lock lock) {
            // TODO(proj4_part1): implement
            for (Lock l : locks) {
                if (Objects.equals(l.transactionNum, lock.transactionNum)) {
                    // find the lock held by the same transaction, update it
                    l.lockType = lock.lockType;
                    return;
                }
            }
            this.locks.add(lock);
        }

        /**
         * Releases the lock `lock` and processes the queue. Assumes that the
         * lock has been granted before.
         */
        public void releaseLock(Lock lock) {
            // TODO(proj4_part1): implement
            locks.removeIf(l -> l.transactionNum.equals(lock.transactionNum)
                    && l.name.equals(lock.name));

            Iterator<LockRequest> iter = waitingQueue.iterator();
            while (iter.hasNext()) {
                LockRequest request = iter.next();
                if (checkCompatible(request.lock.lockType, request.lock.transactionNum)) {
                    // 可以授予锁
                    grantOrUpdateLock(request.lock);
                    iter.remove(); // 从等待队列中移除

                    // 释放请求中指定的锁
                    for (Lock toRelease : request.releasedLocks) {
                        releaseLock(toRelease);
                    }

                    // 解锁事务
                    request.transaction.unblock();
                }
            }
        }

        /**
         * Adds `request` to the front of the queue if addFront is true, or to
         * the end otherwise.
         */
        public void addToQueue(LockRequest request, boolean addFront) {
            // TODO(proj4_part1): implement
            if (addFront) {
                waitingQueue.addFirst(request);
            } else {
                waitingQueue.addLast(request);
            }
        }

        /**
         * Grant locks to requests from front to back of the queue, stopping
         * when the next lock cannot be granted. Once a request is completely
         * granted, the transaction that made the request can be unblocked.
         */
        private void processQueue() {
            Iterator<LockRequest> requests = waitingQueue.iterator();

            // TODO(proj4_part1): implement
            while (requests.hasNext()) {
                LockRequest re = requests.next();
                if (checkCompatible(re.lock.lockType, re.lock.transactionNum)) {
                    // 可以授予锁
                    grantOrUpdateLock(re.lock);
                    requests.remove(); // 从等待队列中移除
                    // 释放请求中指定的锁
                    for (Lock toRelease : re.releasedLocks) {
                        releaseLock(toRelease);
                    }
                    // 唤醒事务
                    re.transaction.unblock();
                } else {
                    break; // 队列中下一个请求无法满足，停止处理
                }
            }
        }

        /**
         * Gets the type of lock `transaction` has on this resource.
         */
        public LockType getTransactionLockType(long transaction) {
            // TODO(proj4_part1): implement
            for (Lock l : this.locks) {
                if (l.transactionNum == transaction) {
                    return l.lockType;
                }
            }
            return LockType.NL;
        }

        /**
         * Returns the list of locks held by `transaction` on this resource.
         */
        public List<Lock> getTransactionLocks(long transaction, ResourceName name) {
            List<Lock> locks = new ArrayList<>();
            if (this.locks == null) {
                return locks;
            }

            for (Lock lock : this.locks) {
                if (lock.name.equals(name) && lock.transactionNum == transaction) {
                    locks.add(lock);
                }
            }
            return locks;
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                    ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<String, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to `name`.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`, and
     * releases all locks on `releaseNames` held by the transaction after
     * acquiring the lock in one atomic action.
     *
     * Error checking must be done before any locks are acquired or released. If
     * the new lock is not compatible with another transaction's lock on the
     * resource, the transaction is blocked and the request is placed at the
     * FRONT of the resource's queue.
     *
     * Locks on `releaseNames` should be released only after the requested lock
     * has been acquired. The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on `name` should NOT
     * change the acquisition time of the lock on `name`, i.e. if a transaction
     * acquired locks in the order: S(A), X(B), acquire X(A) and release S(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is already held
     * by `transaction` and isn't being released
     * @throws NoLockHeldException if `transaction` doesn't hold a lock on one
     * or more of the names in `releaseNames`
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseNames)
            throws DuplicateLockRequestException, NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep
        // all your code within the given synchronized block and are allowed to
        // move the synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        LockRequest request = null;
        synchronized (this) {
            long transNum = transaction.getTransNum();
            ResourceEntry targetEntry = getResourceEntry(name);
            // 1. Error checking
            // 1.1 DuplicateLockRequestException
            LockType currentLockType = targetEntry.getTransactionLockType(transNum);
            if (currentLockType != LockType.NL && !releaseNames.contains(name)) {
                throw new DuplicateLockRequestException("Transaction " + transNum +
                        " already has a lock on " + name);
            }

            // 1.2 NoLockHeldException
            for (ResourceName r : releaseNames) {
                ResourceEntry entry = getResourceEntry(r);
                LockType heldLockType = entry.getTransactionLockType(transNum);
                if (heldLockType == LockType.NL) {
                    throw new NoLockHeldException("Transaction " + transNum +
                            " does not hold a lock on " + r);
                }
            }

            // 2. Check if we can grant the lock
            if (!targetEntry.checkCompatible(lockType, transNum)) {
                // 2.1 Cannot grant the lock, add to queue and block
                Lock lock = new Lock(name, lockType, transNum);
                List<Lock> releasedLocks = new ArrayList<>();
                for (ResourceName rn : releaseNames) {
                    releasedLocks.addAll(getResourceEntry(rn).getTransactionLocks(transNum, rn));
                }

                request = new LockRequest(transaction, lock, releasedLocks);
                targetEntry.addToQueue(request, true); // add to front of queue
                shouldBlock = true;
            } else {
                Lock newLock = new Lock(name, lockType, transNum);
                targetEntry.grantOrUpdateLock(newLock);
                transactionLocks.computeIfAbsent(transNum, k -> new ArrayList<>()).add(newLock);

                // 释放旧锁
                for (ResourceName r : releaseNames) {
                    if (r.equals(name)) continue;

                    ResourceEntry entry = getResourceEntry(r);
                    Lock toRelease = null;
                    for (Lock l : entry.locks) {
                        if (l.transactionNum == transNum) {
                            toRelease = l;
                            break;
                        }
                    }
                    if (toRelease != null) {
                        entry.releaseLock(toRelease);
                        transactionLocks.get(transNum).remove(toRelease);
                    }
                }
            }

            // 3. 如果需要阻塞，准备阻塞
            if (shouldBlock) {
                transaction.prepareBlock();
            }
        }

        // 同步块外阻塞
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is held by
     * `transaction`
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block and are allowed to move the
        // synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        synchronized (this) {
            // error checking
            long transNum = transaction.getTransNum();
            ResourceEntry entry = getResourceEntry(name);

            // 检查是否已经持有锁
            for (Lock l : entry.locks) {
                if (l.transactionNum.equals(transNum)) {
                    throw new DuplicateLockRequestException("Transaction " + transNum +
                            " already has a lock on " + name);
                }
            }

            // 检查是否兼容
            boolean compatible = entry.checkCompatible(lockType, transNum);

            // 已有等待队列或不兼容
            if (!compatible || !entry.waitingQueue.isEmpty()) {
                shouldBlock = true; // 阻塞
                // 加入末尾
                LockRequest request = new LockRequest(transaction, new Lock(name, lockType, transNum));
                entry.waitingQueue.addLast(request);
                transaction.prepareBlock();
            } else {
                // 授予锁
                Lock newLock = new Lock(name, lockType, transNum);
                entry.grantOrUpdateLock(newLock);

                // 更新事务--锁映射
                transactionLocks.putIfAbsent(transNum, new ArrayList<>());
                transactionLocks.get(transNum).add(newLock);
            }
        }

        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Release `transaction`'s lock on `name`. Error checking must be done
     * before the lock is released.
     *
     * The resource name's queue should be processed after this call. If any
     * requests in the queue have locks to be released, those should be
     * released, and the corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     */
    public void release(TransactionContext transaction, ResourceName name)
            throws NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        synchronized (this) {
            // error checking
            long transNum = transaction.getTransNum();
            ResourceEntry entry = getResourceEntry(name);
            Lock heldLock = null;
            for (Lock l : entry.locks) {
                if (l.transactionNum.equals(transNum)) {
                    heldLock = l;
                    break;
                }
            }

            if (heldLock == null) {
                throw new NoLockHeldException("Transaction " + transNum +
                        " does not hold a lock on " + name);
            }

            // 释放锁
            entry.locks.remove(heldLock);

            // 全局释放该锁
            transactionLocks.get(transNum).remove(heldLock);

            // 处理等待队列
            entry.processQueue();
        }
    }

    /**
     * Promote a transaction's lock on `name` to `newLockType` (i.e. change
     * the transaction's lock on `name` from the current lock type to
     * `newLockType`, if its a valid substitution).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the
     * transaction is blocked and the request is placed at the FRONT of the
     * resource's queue.
     *
     * A lock promotion should NOT change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock on `name`
     * @throws NoLockHeldException if `transaction` has no lock on `name`
     * @throws InvalidLockException if the requested lock type is not a
     * promotion. A promotion from lock type A to lock type B is valid if and
     * only if B is substitutable for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        boolean shouldBlock = false;
        synchronized (this) {
            // error checking
            // 1. 检查是否持有锁
            long transNum = transaction.getTransNum();
            ResourceEntry entry = getResourceEntry(name);
            LockType currentLockType = entry.getTransactionLockType(transNum);

            // 已有相同类型的锁
            if (currentLockType.equals(newLockType)) {
                throw new DuplicateLockRequestException("Transaction " + transNum +
                        " already has a lock of type " + newLockType + " on " + name);
            }

            // 没有持有锁
            if (currentLockType == LockType.NL) {
                throw new NoLockHeldException("Transaction " + transNum +
                        " does not hold a lock on " + name);
            }

            // 不是升级
            if (!LockType.substitutable(newLockType, currentLockType)) {
                throw new InvalidLockException("Cannot promote lock from " +
                        currentLockType + " to " + newLockType);
            }

            // 不允许升级成SIX锁
            if (newLockType.equals(LockType.SIX)) {
                acquireAndRelease(transaction, name, newLockType,
                        Collections.singletonList(name));
            }

            // 检查是否兼容
            if (!entry.checkCompatible(newLockType, transNum)) {
                // 不能兼容，加入等待队列
                shouldBlock = true;
                LockRequest request = new LockRequest(transaction,
                        new Lock(name, newLockType, transNum),
                        Collections.emptyList());
                entry.addToQueue(request, true); // 加入队列头
                transaction.prepareBlock(); // 准备阻塞
            } else {
                // 可以兼容，直接升级
                for (Lock l : entry.locks) {
                    if (l.transactionNum == transNum) {
                        l.lockType = newLockType;
                        break;
                    }
                }

                // 更新全局映射
                List<Lock> locks = transactionLocks.get(transNum);
                for (Lock l : locks) {
                    if (l.name.equals(name)) {
                        l.lockType = newLockType;
                        break;
                    }
                }
            }
        }

        // 同步块外阻塞
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Return the type of lock `transaction` has on `name` or NL if no lock is
     * held.
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        // TODO(proj4_part1): implement
        ResourceEntry resourceEntry = getResourceEntry(name);
        long transNum = transaction.getTransNum();
        for (Lock l : resourceEntry.locks) {
            if (l.transactionNum == transNum) {
                return l.lockType;
            }
        }
        return LockType.NL;
    }

    /**
     * Returns the list of locks held on `name`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks held by `transaction`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at the top of this file and the top
     * of LockContext.java for more information.
     */
    public synchronized LockContext context(String name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, name));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at the top of this
     * file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database");
    }
}

package simpledb.Lock;

import simpledb.storage.PageId;
import simpledb.transaction.TransactionId;

import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, PageLock>> lockMap;


    public LockManager() {
        lockMap = new ConcurrentHashMap<>();
    }

    public synchronized boolean acquiredLock(TransactionId transactionId, PageId pageId, int type) {
        ConcurrentHashMap<TransactionId, PageLock> locks = lockMap.get(pageId);
        if (locks == null) {
            PageLock pageLock = new PageLock(transactionId, type);
            ConcurrentHashMap<TransactionId, PageLock> lockConcurrentHashMap = new ConcurrentHashMap<>();
            lockConcurrentHashMap.put(transactionId, pageLock);
            lockMap.put(pageId, lockConcurrentHashMap);
            return true;
        }
        PageLock pageLock = locks.get(transactionId);
        if (pageLock == null) {
            if (locks.size() > 1) {
                if (type == PageLock.SHARED) {
                    PageLock pageLock1 = new PageLock(transactionId, type);
                    locks.put(transactionId, pageLock1);
                    lockMap.put(pageId, locks);
                    return true;
                } else if (type == PageLock.EXCLUSIVE) {
                    return false;
                }

            } else if (locks.size() == 1) {
                PageLock curLock = null;
                for (PageLock lock : locks.values()) {
                    curLock = lock;
                }
                if (curLock.getType() == PageLock.SHARED) {
                    if (type == PageLock.SHARED) {
                        PageLock newPageLock = new PageLock(transactionId, type);
                        locks.put(transactionId, newPageLock);
                        lockMap.put(pageId, locks);
                        return true;
                    } else if (type == PageLock.EXCLUSIVE) {
                        return false;
                    }

                } else if (curLock.getType() == PageLock.EXCLUSIVE) {
                    return false;
                }

            }

        } else if (pageLock != null) {
            if (pageLock.getType() == PageLock.SHARED) {
                if (type == PageLock.SHARED) {
                    return true;
                } else if (type == PageLock.EXCLUSIVE) {
                    if (locks.size() == 1) {
                        pageLock.setType(PageLock.EXCLUSIVE);
                        locks.put(transactionId, pageLock);
                        lockMap.put(pageId,locks);
                        return true;
                    } else if (locks.size() > 1) {
                        // 不能进行锁的升级
                        return false;
                    }
                }

            }
            return pageLock.getType() == PageLock.EXCLUSIVE;
        }
        return false;

    }

    public synchronized boolean isHoldLock(TransactionId transactionId, PageId pageId) {
        ConcurrentHashMap<TransactionId, PageLock> transactionIdPageLockConcurrentHashMap = lockMap.get(pageId);
        if (transactionIdPageLockConcurrentHashMap == null) {
            return false;
        }
        PageLock pageLock = transactionIdPageLockConcurrentHashMap.get(transactionId);
        if (pageLock == null) {
            return false;
        }
        return true;

    }

    public synchronized boolean releaseLock(TransactionId transactionId, PageId pageId) {
        if (isHoldLock(transactionId, pageId)) {
            ConcurrentHashMap<TransactionId, PageLock> transactionIdPageLockConcurrentHashMap = lockMap.get(pageId);

            transactionIdPageLockConcurrentHashMap.remove(transactionId);
            if (transactionIdPageLockConcurrentHashMap.size() == 0) {
                lockMap.remove(pageId);
            }

            return true;
        }
        return false;
    }

    public synchronized void completeTransaction(TransactionId transactionId) {
        ConcurrentHashMap.KeySetView<PageId, ConcurrentHashMap<TransactionId, PageLock>> pageIds = lockMap.keySet();
        for (PageId pageId : pageIds) {
            releaseLock(transactionId, pageId);
        }
    }
}

package simpledb.Lock;

import simpledb.storage.PageId;
import simpledb.transaction.TransactionId;

import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    // 用于存放所有的page上的所有的锁，对于同一个page上的锁，通过事务tid来获取到该事务的锁
    ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, PageLock>> lockMap;

    public LockManager() {
        lockMap = new ConcurrentHashMap<>();
    }

    public synchronized boolean acquiredLock(TransactionId transactionId, PageId pageId, int type) {
        // 先判断是否存在锁，如果不存在则可以直接申请持有一个锁
        ConcurrentHashMap<TransactionId, PageLock> locks = lockMap.get(pageId);
        if (locks == null) {
            // 全局一个锁都无，可以直接申请持有
            PageLock pageLock = new PageLock(transactionId, type);
            ConcurrentHashMap<TransactionId, PageLock> lockConcurrentHashMap = new ConcurrentHashMap<>();
            lockConcurrentHashMap.put(transactionId, pageLock);
            lockMap.put(pageId, lockConcurrentHashMap);
            return true;
        }
        // 判断当前事务之前是否在该page上持有锁
        PageLock pageLock = locks.get(transactionId);
        if (pageLock == null) {
            // 当前page上不存在该事务的锁，直接申请新的锁
            if (locks.size() > 1) {
                // 如果 size() > 1则证明存在多个锁，且为多个共享锁，如果当前事务要申请共享锁，直接进行添加，排他锁则拒绝
                if (type == PageLock.SHARED) {
                    PageLock pageLock1 = new PageLock(transactionId, type);
                    locks.put(transactionId, pageLock1);
                    lockMap.put(pageId, locks);
                    return true;
                } else if (type == PageLock.EXCLUSIVE) {
                    return false;
                }

            } else if (locks.size() == 1) {
                // 当前page上只有一个锁，可能为共享锁也可以为排他锁
                PageLock curLock = null;
                for (PageLock lock : locks.values()) {
                    curLock = lock;
                }
                // 获取锁用于判断类型
                if (curLock.getType() == PageLock.SHARED) {
                    // 如果是共享锁并且该事务也为共享锁则可以申请持有
                    if (type == PageLock.SHARED) {
                        PageLock newPageLock = new PageLock(transactionId, type);
                        locks.put(transactionId, newPageLock);
                        lockMap.put(pageId, locks);
                        return true;
                    } else if (type == PageLock.EXCLUSIVE) {
                        // 当前事务为排他锁，拒绝
                        return false;
                    }

                } else if (curLock.getType() == PageLock.EXCLUSIVE) {
                    // page上的锁为排他锁，直接拒绝
                    return false;
                }

            }

        } else if (pageLock != null) {
            // 之前存在过锁，需要考虑锁的升级
            if (pageLock.getType() == PageLock.SHARED) {
                if (type == PageLock.SHARED) {
                    // 新锁为共享锁，直接返回
                    return true;
                } else if (type == PageLock.EXCLUSIVE) {
                    // 当前事务为排他锁，如果该page上没有其他事务持有的锁，则可以进行申请
                    if (locks.size() == 1) {
                        pageLock.setType(PageLock.EXCLUSIVE);
                        locks.put(transactionId, pageLock);
                        lockMap.put(pageId,locks);
                        return true;
                    } else if (locks.size() > 1) {
                        // 其他事务持有了共享锁，存在冲突，不能进行锁的升级
                        return false;
                    }
                }

            }
            // 之前为写锁，无论如何都无需进行升级，新事务为写锁则直接返回，新事务为共享锁则拒绝
            return pageLock.getType() == PageLock.EXCLUSIVE;
        }
        return false;

    }

    public synchronized boolean isHoldLock(TransactionId transactionId, PageId pageId) {
        ConcurrentHashMap<TransactionId, PageLock> transactionIdPageLockConcurrentHashMap = lockMap.get(pageId);
        //如果该page上无锁则可以直接返回
        if (transactionIdPageLockConcurrentHashMap == null) {
            return false;
        }
        // 尝试根据id来获取锁
        PageLock pageLock = transactionIdPageLockConcurrentHashMap.get(transactionId);
        if (pageLock == null) {
            return false;
        }
        return true;

    }

    public synchronized boolean releaseLock(TransactionId transactionId, PageId pageId) {
        // 有锁则逐级释放
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
        // 遍历所有的page，清除掉与当前事务有联系的锁
        for (PageId pageId : pageIds) {
            releaseLock(transactionId, pageId);
        }
    }
}

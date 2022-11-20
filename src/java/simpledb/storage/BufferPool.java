package simpledb.storage;

import simpledb.Lock.LockManager;
import simpledb.Lock.PageLock;
import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    // 页面的最大数量
    private final int numPages;
    // 储存的页面
    private final ConcurrentHashMap<PageId, LinkedNode> pageStore;

    private class LinkedNode {
        PageId pageId;
        Page page;
        LinkedNode prev;
        LinkedNode next;
        // head与tail均为dummy节点，仅为表示，不真实存储数据


        public LinkedNode() {

        }

        public LinkedNode(PageId pageId, Page page) {
            this.pageId = pageId;
            this.page = page;
        }


    }

    LinkedNode head;
    LinkedNode tail;

    private void addToHead(LinkedNode node) {
        node.prev = head;
        node.next = head.next;
        head.next.prev = node;
        head.next = node;

    }

    private void remove(LinkedNode node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
    }

    private void moveToHead(LinkedNode node) {
        remove(node);
        addToHead(node);
    }

    private LinkedNode removeTail() {
        LinkedNode node = tail.prev;
        remove(node);
        return node;
    }

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */

    private LockManager lockManager;

    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.pageStore = new ConcurrentHashMap<>();
        head = new LinkedNode(new HeapPageId(-1, -1), null);
        tail = new LinkedNode(new HeapPageId(-1, -1), null);
        head.next = tail;
        tail.prev = head;
        this.lockManager = new LockManager();

    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        int type;
        if (perm == Permissions.READ_ONLY) {
            type = PageLock.SHARED;
        } else {
            type = PageLock.EXCLUSIVE;
        }
        // 先判断事务的类型，来获取对应类型的锁
        long startTime = System.currentTimeMillis();
        // 设置超时时间为500ms
        boolean isAcquired = false;

        while (!isAcquired) {
            isAcquired = lockManager.acquiredLock(tid, pid, type);
            long now = System.currentTimeMillis();
            if (now - startTime > 500) {
                throw new TransactionAbortedException();
            }
        }
        // some code goes here
        // 如果缓存池中没有
        if (!pageStore.containsKey(pid)) {
            // 获取
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = dbFile.readPage(pid);
            // 是否超过大小
            if (pageStore.size() >= numPages) {
                // 淘汰 (后面的 Exercise 书写)
                evictPage();
            }
            // 放入缓存

            LinkedNode linkedNode = new LinkedNode(page.getId(), page);
            pageStore.put(pid, linkedNode);
            addToHead(linkedNode);

        }
        // 从 缓存池 中获取
        return pageStore.get(pid).page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for Exercise1|Exercise2
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for Exercise1|Exercise2
        transactionComplete(tid, true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for Exercise1|Exercise2

        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for Exercise1|Exercise2
        if (commit) {
            try {
                flushPages(tid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            restorePages(tid);
        }
        lockManager.completeTransaction(tid);


    }


    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for Exercise2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pages = heapFile.insertTuple(tid, t);
        updateBufferPool(pages, tid);
        // not necessary for Exercise1
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        PageId pageId = t.getRecordId().getPageId();
        int tableId = pageId.getTableId();
        HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> pages = heapFile.deleteTuple(tid, t);
        updateBufferPool(pages, tid);

        // some code goes here
        // not necessary for Exercise1
    }

    public void updateBufferPool(List<Page> pages, TransactionId tid) {
        for (Page page : pages) {
            page.markDirty(true, tid);

            if (pageStore.size() > numPages) {
                try {
                    evictPage();
                } catch (DbException e) {
                    e.printStackTrace();
                }
            }
            PageId pageId = page.getId();
            LinkedNode linkedNode = new LinkedNode(pageId,page);
            pageStore.put(pageId,linkedNode);
        }
    }


    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        ConcurrentHashMap.KeySetView<PageId, LinkedNode> pageIds = pageStore.keySet();
        for (PageId pageId : pageIds) {
            flushPage(pageId);
        }
        // not necessary for Exercise1

    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        remove(pageStore.get(pid));
        pageStore.remove(pid);
        // not necessary for Exercise1
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        Page page = pageStore.get(pid).page;
        if (page.isDirty() != null) {
            int tableId = page.getId().getTableId();
            Database.getCatalog().getDatabaseFile(tableId).writePage(page);
            page.markDirty(false,null);
        }

        // not necessary for Exercise1
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for Exercise1|Exercise2
        for (LinkedNode linkedNode : pageStore.values()) {
            Page page = linkedNode.page;
            PageId pageId = linkedNode.pageId;
            if (tid.equals(page.isDirty())) {
                flushPage(pageId);
            }
        }
    }

    public synchronized void restorePages(TransactionId tid) {
        for (LinkedNode linkedNode : pageStore.values()) {
            Page page = linkedNode.page;
            PageId pageId = linkedNode.pageId;
            if (tid.equals(page.isDirty())) {
                int tableId = pageId.getTableId();
                DbFile databaseFile = Database.getCatalog().getDatabaseFile(tableId);
                Page pageFromDisk = databaseFile.readPage(pageId);

                linkedNode.page = pageFromDisk;
                pageStore.put(pageId,linkedNode);
                moveToHead(linkedNode);
            }


        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for Exercise1
        for (int i = 0; i < numPages; i++) {
            LinkedNode linkedNode = removeTail();
            Page page = linkedNode.page;
            if (page.isDirty() != null) {
                addToHead(linkedNode);
            } else {
                try {
                    flushPage(linkedNode.pageId);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                pageStore.remove(linkedNode.pageId);
                return;
            }
        }
        throw new DbException("all pages are dirty page");

    }
}

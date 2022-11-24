package simpledb.storage.evict;

import simpledb.storage.Page;
import simpledb.storage.PageId;

public class DLinkedNode {
    PageId pageId;
    DLinkedNode prev;
    DLinkedNode next;
    public DLinkedNode () {}
    public DLinkedNode(PageId pageId) {
        this.pageId = pageId;
    }
    public PageId getPageId() {
        return this.pageId;
    }
}

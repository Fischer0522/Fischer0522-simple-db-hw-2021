package simpledb.Lock;

import simpledb.storage.PageId;
import simpledb.transaction.TransactionId;

public class PageLock {
    public static final int SHARED = 0;
    public static final int EXCLUSIVE = 1;

    private TransactionId transactionId;
    private int type;

    public PageLock(TransactionId transactionId, int type) {
        this.transactionId = transactionId;
        this.type = type;
    }

    public TransactionId getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(TransactionId transactionId) {
        this.transactionId = transactionId;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }
}

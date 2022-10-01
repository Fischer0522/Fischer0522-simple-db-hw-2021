package simpledb.execution;

import simpledb.common.Database;
import simpledb.storage.DbFile;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Type;
import simpledb.common.DbException;
import simpledb.storage.DbFileIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;
    private final TransactionId tid;
    private  int tableId;
    private  String tableAlias;
    private DbFileIterator it;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid = tid;
        this.tableId = tableid;
        this.tableAlias = tableAlias;
        // some code goes here
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {

        return Database.getCatalog().getTableName(this.tableId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // some code goes here
        return this.tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        this.tableId = tableid;
        this.tableAlias = tableAlias;
        // some code goes here
    }
    // 默认不起别名的情况，则使用表原始名字
    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        it = Database.getCatalog().getDatabaseFile(tableId).iterator(tid);
        it.open();
        // some code goes here

    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    @Override
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc tupleDesc = Database.getCatalog().getTupleDesc(this.tableId);
        List<TupleDesc.TDItem> list = new ArrayList<>();
        for (TupleDesc.TDItem tdItem : tupleDesc.getTdItems()) {
            TupleDesc.TDItem item = new TupleDesc.TDItem(tdItem.fieldType,tableAlias+"."+tdItem.fieldName);
            list.add(item);
        }

        return new TupleDesc(list);

    }

    @Override
    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (it == null) {
            return false;
        }
        return it.hasNext();
    }

    @Override
    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        if (it == null) {
            throw new NoSuchElementException("no next tuple");
        }
        Tuple tuple = it.next();
        if (tuple == null) {
            throw new NoSuchElementException("no next tuple");
        }
        return tuple;
    }

    @Override
    public void close() {
        it = null;
    }

    @Override
    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        it.rewind();
        // some code goes here
    }
}

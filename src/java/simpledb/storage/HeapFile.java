package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private File f;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
        // some code goes here
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    @Override
    public int getId() {
        // some code goes here
        return f.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    @Override
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    @Override
    public Page readPage(PageId pid) {
        long offset = (long) pid.getPageNumber() * BufferPool.getPageSize();
        byte[] data = new byte[BufferPool.getPageSize()];
        try {
            RandomAccessFile rFile = new RandomAccessFile(this.f, "r");
            rFile.seek(offset);
            for (int i = 0; i < BufferPool.getPageSize(); i++) {
                data[i] = (byte) rFile.read();
            }
            HeapPage page = new HeapPage((HeapPageId) pid, data);
            rFile.close();
            return page;
        } catch (Exception e) {
            return null;
        }

        // some code goes here
    }

    // see DbFile.java for javadocs
    @Override
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        long length = f.length();
        int pageSize = BufferPool.getPageSize();
        return (int)length / pageSize;

        // some code goes here
    }

    // see DbFile.java for javadocs
    @Override
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    @Override
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    @Override
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this,tid);
    }

    private static final class HeapFileIterator implements DbFileIterator {
        private  final HeapFile heapFile;
        private static Iterator<Tuple> it;
        private final TransactionId transactionId;
        private int whichPage;

        public HeapFileIterator(HeapFile heapFile,TransactionId transactionId) {
            this.transactionId = transactionId;
            this.heapFile = heapFile;
        }
        @Override
        public void open() throws DbException, TransactionAbortedException {
            whichPage = 0;
            it = getPageTuples(whichPage);
        }

        private Iterator<Tuple> getPageTuples(int pageNumber) throws TransactionAbortedException{
            if (pageNumber >=0 && pageNumber < heapFile.numPages()) {
                HeapPageId heapPageId = new HeapPageId(heapFile.getId(),pageNumber);
                try {
                    HeapPage page = (HeapPage) Database.getBufferPool().getPage(transactionId, heapPageId, Permissions.READ_ONLY);
                    return page.iterator();
                } catch (DbException e) {
                    e.printStackTrace();
                    try {
                        throw new DbException(String.format("heapfile %d does not contain page %d!", pageNumber,heapFile.getId()));
                    } catch (DbException ex) {
                        ex.printStackTrace();
                    }
                }
            }
            return null;

        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (it == null) {
                return false;
            }
            if (!it.hasNext()) {
                if (whichPage <heapFile.numPages() -1) {
                    whichPage++;
                    it = getPageTuples(whichPage);
                    return it.hasNext();
                } else {
                    return false;
                }
            } else {
                return true;
            }

        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (it == null || !it.hasNext()) {
                throw new NoSuchElementException();
            }
            return it.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();

        }

        @Override
        public void close() {
            it = null;

        }
    }


}


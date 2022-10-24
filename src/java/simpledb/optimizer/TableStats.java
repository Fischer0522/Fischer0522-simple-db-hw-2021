package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IO_COST_PER_PAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IO_COST_PER_PAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;
    private DbFile dbFile;
    private int ioCostPerPage;
    private TupleDesc tupleDesc;
    private int tableId;
    private int pageNum;
    private int tupleNum;
    private int fieldNum;
    Map<Integer,IntHistogram> intHistogramMap;
    Map<Integer,StringHistogram> stringHistogramMap;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        this.tableId = tableid;
        this.ioCostPerPage = ioCostPerPage;
        this.dbFile = Database.getCatalog().getDatabaseFile(tableid);
        this.tupleDesc = dbFile.getTupleDesc();
        HeapFile heapFile = (HeapFile) dbFile;
        this.pageNum = heapFile.numPages();
        intHistogramMap = new HashMap<>();
        stringHistogramMap = new HashMap<>();
        Type[] type = getType(tupleDesc);
        this.fieldNum = type.length;
        this.tupleNum = 0;
        TransactionId transactionId = new TransactionId();
        SeqScan seqScan = new SeqScan(transactionId,tableId);
        int[] maxs = new int[fieldNum];
        int[] mins = new int[fieldNum];
        try {
            seqScan.open();

            for (int i = 0;i < fieldNum;i++) {

                int max = Integer.MIN_VALUE;
                int min = Integer.MAX_VALUE;
                while (seqScan.hasNext()) {
                    if(i == 0) {
                        tupleNum++;
                    }
                    Tuple tuple = seqScan.next();
                    Field field = tuple.getField(i);
                    Type type1 = field.getType();
                    if (type1 == Type.STRING_TYPE) {
                        continue;
                    }
                    IntField intField = (IntField) field;
                    int val = intField.getValue();
                    max = Math.max(max,val);
                    min = Math.min(min,val);

                }
                maxs[i] = max;
                mins[i] = min;
                seqScan.rewind();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            seqScan.close();
        }
        for (int i = 0; i < fieldNum;i++) {
            if (type[i] == Type.INT_TYPE) {
                IntHistogram intHistogram = new IntHistogram(NUM_HIST_BINS,mins[i],maxs[i]);
                intHistogramMap.put(i,intHistogram);
            } else {
                StringHistogram stringHistogram = new StringHistogram(NUM_HIST_BINS);
                stringHistogramMap.put(i,stringHistogram);
            }
        }

        addValueToHist();





        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
    }

    private Type[] getType(TupleDesc tupleDesc) {
        int numFields = tupleDesc.numFields();
        Type[] types = new Type[numFields];
        for (int i = 0; i <numFields;i++) {
            types[i] = tupleDesc.getFieldType(i);
        }
        return types;

    }
    private void addValueToHist(){
        TransactionId tid = new TransactionId();
        SeqScan seqScan = new SeqScan(tid,tableId);
        try {
            seqScan.open();
            while (seqScan.hasNext()) {
                Tuple tuple = seqScan.next();
                for (int i = 0; i < fieldNum;i++) {
                    Field field = tuple.getField(i);
                    Type type = field.getType();
                    if (type == Type.INT_TYPE) {
                        IntField intField = (IntField) field;
                        int value = intField.getValue();
                        intHistogramMap.get(i).addValue(value);
                    } else {
                        StringField stringField = (StringField) field;
                        String value = stringField.getValue();
                        stringHistogramMap.get(i).addValue(value);
                    }
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            seqScan.close();
        }

    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return pageNum * ioCostPerPage;



    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        double v = selectivityFactor * tupleNum;
        return (int) v;

    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        Type[] type = getType(tupleDesc);
        Type type1 = type[field];
        if (type1 == Type.INT_TYPE) {
            return intHistogramMap.get(field).avgSelectivity();
        } else {
            return stringHistogramMap.get(field).avgSelectivity();
        }

    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        Type fieldType = tupleDesc.getFieldType(field);
        if (fieldType == Type.INT_TYPE) {
            IntField field1 = (IntField) constant;
            int value = field1.getValue();
            return intHistogramMap.get(field).estimateSelectivity(op,value);
        } else {
            StringField stringField = (StringField) constant;
            String value = stringField.getValue();
            return stringHistogramMap.get(field).estimateSelectivity(op,value);

        }

    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return this.tupleNum;
    }

}

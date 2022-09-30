package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Map<Field, Integer> group;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if(!what.equals(Op.COUNT)){
            throw new IllegalArgumentException("String类型只支持计数");
        }
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        group = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    @Override
    public void mergeTupleIntoGroup(Tuple tup) {
        Field fieldToGroupBy = tup.getField(gbfield);
        if (this.gbfield == NO_GROUPING) {
            fieldToGroupBy =null;
        }
        Integer orDefault = group.getOrDefault(fieldToGroupBy, 0);
        orDefault++;
        group.put(fieldToGroupBy,orDefault);

        // some code goes here
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    @Override
    public OpIterator iterator() {
        List<Tuple> tuples = new ArrayList<>();
        Type[] types;
        String[] names;
        if (this.gbfield == NO_GROUPING) {
            types = new Type[]{Type.INT_TYPE};
            names = new String[]{"aggregateVal"};
            TupleDesc tupleDesc = new TupleDesc(types,names);
            Tuple tuple = new Tuple(tupleDesc);
            Integer integer = group.get(null);
            IntField intField = new IntField(integer);
            tuple.setField(0,intField);
            tuples.add(0,tuple);
            return new TupleIterator(tupleDesc,tuples);
        } else {
            types = new Type[]{gbfieldtype,Type.INT_TYPE};
            names = new String[]{"groupVal","aggregateVal"};
            TupleDesc tupleDesc = new TupleDesc(types,names);

            Set<Field> fields1 = group.keySet();
            for(Field field : fields1) {
                Tuple tuple = new Tuple(tupleDesc);
                Integer integer = group.get(field);
                tuple.setField(0,field);
                IntField intField = new IntField(integer);
                tuple.setField(1,intField);
                tuples.add(tuple);
            }
            return new TupleIterator(tupleDesc,tuples);
        }


    }

}

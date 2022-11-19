package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private AggHandler aggHandler;

    private abstract class AggHandler{
        Map<Field,Integer> group;

        abstract void handle(Tuple tuple);

        Map<Field,Integer> getGroup() {
            return this.group;
        }
    }

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;

        if (this.what == Op.MIN) {
            aggHandler = new MinHandler();

        } else if (this.what == Op.MAX) {
            aggHandler = new MaxHandler();

        } else if (this.what == Op.SUM) {
            aggHandler = new SumHandler();

        } else if (this.what == Op.AVG) {
            aggHandler = new AvgHandler();

        } else if (this.what == Op.COUNT) {
            aggHandler = new CountHandler();

        }


        // some code goes here
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    @Override
    public void mergeTupleIntoGroup(Tuple tup) {
        aggHandler.handle(tup);

        // some code goes here
    }


    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */

    // 迭代聚合后的结果
    @Override
    public OpIterator iterator() {
        Map<Field, Integer> group = aggHandler.getGroup();
        List<Tuple> tuples = new ArrayList<>();
        Type[] types;
        String[] names;
        if(this.gbfield == NO_GROUPING) {
            types = new Type[]{Type.INT_TYPE};
            names = new String[]{"aggregateVal"};
            TupleDesc tupleDesc = new TupleDesc(types,names);
            Tuple tuple = new Tuple(tupleDesc);
            Integer integer = group.get(null);
            Field intField = new IntField(integer);
            tuple.setField(0,intField);
            tuples.add(tuple);
            return new TupleIterator(tupleDesc,tuples);
        } else {
            types = new Type[]{gbfieldtype,Type.INT_TYPE};
            names = new String[]{"groupVal","aggregateVal"};
            TupleDesc tupleDesc = new TupleDesc(types,names);

            Set<Field> fields = group.keySet();
            for(Field field : fields) {
                // 对于每个group生成一条tuple
                Tuple tuple = new Tuple(tupleDesc);
                Integer integer = group.get(field);
                IntField intField = new IntField(integer);
                tuple.setField(0,field);
                tuple.setField(1,intField);
                tuples.add(tuple);
            }
            return new TupleIterator(tupleDesc,tuples);

        }
        // some code goes here

    }

   private class MinHandler extends AggHandler {
        public MinHandler(){
            group = new HashMap<>();
        }
       @Override
       void handle(Tuple tuple) {

           Field groupByField = tuple.getField(gbfield);
           if (gbfield == NO_GROUPING) {
                groupByField = null;
           }
           IntField intField = (IntField) tuple.getField(afield);
           int value = intField.getValue();
           Integer orDefault = group.getOrDefault(groupByField, Integer.MAX_VALUE);
           if (value < orDefault) {
               group.put(groupByField,value);
           }
       }
   }

   private class MaxHandler extends  AggHandler {

        public MaxHandler() {
            group = new HashMap<>();
        }
       @Override
       void handle(Tuple tuple) {
           Field groupByField = tuple.getField(gbfield);
           if (gbfield == NO_GROUPING) {
               groupByField = null;
           }
           IntField intField = (IntField) tuple.getField(afield);
           int value = intField.getValue();
           Integer orDefault = group.getOrDefault(groupByField, Integer.MIN_VALUE);
           if (value > orDefault) {
               group.put(groupByField,value);
           }

       }
   }

   private class SumHandler extends AggHandler {

        public SumHandler() {
            group = new HashMap<>();
        }
       @Override
       void handle(Tuple tuple) {
           Field groupByField = tuple.getField(gbfield);
           if (gbfield == NO_GROUPING) {
               groupByField = null;
           }
           IntField intField = (IntField) tuple.getField(afield);
           int value = intField.getValue();
           Integer sum = group.getOrDefault(groupByField, 0);
           value += sum;
           group.put(groupByField,value);
       }
   }

   private class AvgHandler extends AggHandler {
        Map<Field,Integer> sumMap;
        Map<Field,Integer> countMap;
        public AvgHandler() {
            group = new HashMap<>();
            sumMap = new HashMap<>();
            countMap = new HashMap<>();
        }
       @Override
       void handle(Tuple tuple) {
           // 先判断是否为空
           Field groupByField;
           if (gbfield == NO_GROUPING) {
               groupByField = null;
           } else {
               groupByField = tuple.getField(gbfield);
           }

           IntField intField = (IntField) tuple.getField(afield);
           int value = intField.getValue();
           Integer orDefault = sumMap.getOrDefault(groupByField, 0);
           orDefault += value;
           sumMap.put(groupByField,orDefault);
           Integer count = countMap.getOrDefault(groupByField, 0);
           count++;
           countMap.put(groupByField,count);
           group.put(groupByField, orDefault / count);

       }
   }
   private class CountHandler extends AggHandler {
        public CountHandler() {
            group = new HashMap<>();
        }
       @Override
       void handle(Tuple tuple) {
           Field groupByField = tuple.getField(gbfield);
           if (gbfield == NO_GROUPING) {
               groupByField = null;
           }
           Integer orDefault = group.getOrDefault(groupByField, 0);
           orDefault++;
           group.put(groupByField,orDefault);
       }
   }


}

package simpledb.execution;

import simpledb.storage.Field;
import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;
    private JoinPredicate p;
    private OpIterator child1;
    private OpIterator child2;
    private Tuple t;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     *
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // some code goes here
        this.p = p;
        this.child1 = child1;
        this.child2 = child2;
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return this.p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        int field1Pos = p.getField1();
        return child1.getTupleDesc().getFieldName(field1Pos);
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        int field2Pos = p.getField2();
        return child2.getTupleDesc().getFieldName(field2Pos);
    }

    /**
     * @see TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    @Override
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc tupleDesc1 = child1.getTupleDesc();
        TupleDesc tupleDesc2 = child2.getTupleDesc();
        return TupleDesc.merge(tupleDesc1,tupleDesc2);
    }

    @Override
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {

        child1.open();
        child2.open();
        super.open();
        // some code goes here
    }

    @Override
    public void close() {
        super.close();
        child1.close();
        child2.close();
        // some code goes here
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child1.rewind();
        child2.rewind();
        t = null;

    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    @Override
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {

        while (this.child1.hasNext() || t != null){
            if(this.child1.hasNext() && t == null){
                t = this.child1.next();
            }
            while(child2.hasNext()){
                Tuple t2 = child2.next();
                if(p.filter(t, t2)){
                    TupleDesc td1 = t.getTupleDesc();
                    TupleDesc td2 = t2.getTupleDesc();
                    // ??????
                    TupleDesc tupleDesc = TupleDesc.merge(td1, td2);
                    // ???????????????
                    Tuple newTuple = new Tuple(tupleDesc);
                    // ????????????
                    newTuple.setRecordId(t.getRecordId());
                    // ??????
                    int i = 0;
                    for (; i < td1.numFields(); i++) {

                        newTuple.setField(i, t.getField(i));
                    }
                    for (int j = 0; j < td2.numFields(); j++) {

                        newTuple.setField(i + j, t2.getField(j));
                    }
                    // ?????????t2????????????t??????????????????????????????
                    if(!child2.hasNext()){
                        child2.rewind();
                        t = null;
                    }
                    return newTuple;
                }
            }
            // ?????? child2
            child2.rewind();
            t = null;
        }
        return null;


    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here

        return new OpIterator[] {child1,child2};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child1 = children[0];
        this.child2 = children[1];
    }

}

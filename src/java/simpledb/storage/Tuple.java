package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private RecordId recordId;
    private TupleDesc tupleDesc;
    private  List<Field> fields;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        this.tupleDesc =td;
        fields = new ArrayList<>();
        // some code goes here
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here

        return this.tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return this.recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        this.recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // 先检测索引是否符合规范
        int maxSize = this.tupleDesc.getSize();
        if (i < 0 || i >= maxSize) {
            try {
                throw new NoSuchFieldException();
            } catch (NoSuchFieldException e) {
                e.printStackTrace();
            }
        }
        // 判断类型是否匹配
        TupleDesc.TDItem tdItem = tupleDesc.tdItems.get(i);
        if (!f.getType().equals(tdItem.fieldType) )
        {
            throw new UnsupportedOperationException("类型不匹配");
        }
        fields.add(i,f);
        // some code goes here
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {

        int maxSize = this.tupleDesc.getSize();
        if (i < 0 || i >= maxSize) {
            try {
                throw new NoSuchFieldException();
            } catch (NoSuchFieldException e) {
                e.printStackTrace();
            }
        }

        // some code goes here
        return fields.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    @Override
    public String toString() {
        // some code goes here
        throw new UnsupportedOperationException("Implement this");
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return fields.iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here
        if (this.fields == null) {
            this.tupleDesc = td;
        }
        if (tupleDesc.getSize() != fields.size()) {
            throw new UnsupportedOperationException("与field长度不匹配");
        }
        for (int i = 0;i < fields.size();i++) {
            Field currentField = fields.get(i);
            TupleDesc.TDItem targetTd = td.tdItems.get(i);
            if (!currentField.getType().equals(targetTd.fieldType)) {
                // 类型不匹配,尝试强转 但是未提供强转的接口，是否需要自己实现？？？
                try {
                    if (targetTd.fieldType == Type.INT_TYPE &&currentField.getType()==Type.STRING_TYPE) {

                    }

                } catch (Exception e) {

                }
            }
        }

    }
}

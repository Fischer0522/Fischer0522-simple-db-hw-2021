package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    List<TDItem> tdItems;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        @Override
        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }


    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return null;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        tdItems = new ArrayList<>();
        for (int i = 0;i < typeAr.length;i++) {
            TDItem tdItem = new TDItem(typeAr[i],fieldAr[i]);
            tdItems.add(tdItem);
        }
        // some code goes here
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        tdItems = new ArrayList<>();
        for (int i = 0;i < typeAr.length;i++) {
            TDItem tdItem = new TDItem(typeAr[i],null);
            tdItems.add(tdItem);
        }
    }

    public TupleDesc(List<TDItem> list) {
        this.tdItems = list;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return tdItems.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {

        if (Objects.isNull(tdItems)) {
            throw new NoSuchElementException();
        }
        if (i <0 || i >= tdItems.size()) {
            throw new NoSuchElementException();
        }
        // some code goes here
        TDItem tdItem = tdItems.get(i);
        return tdItem.fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (Objects.isNull(tdItems)) {
            throw new NoSuchElementException();
        }
        if (i <0 || i >= tdItems.size()) {
            throw new NoSuchElementException();
        }
        TDItem tdItem = tdItems.get(i);

        // some code goes here
        return tdItem.fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {

        if (Objects.isNull(tdItems)) {
            throw new NoSuchElementException();
        }
        for (int i = 0; i < tdItems.size();i++) {
            TDItem tdItem = tdItems.get(i);
            if (!Objects.isNull(tdItem) ) {
                if (tdItem.fieldName == null) {
                    throw new NoSuchElementException();
                }
                if (tdItem.fieldName.equals(name)) {
                    return i;
                }
            }
        }
        throw new NoSuchElementException();
        // some code goes here
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for (TDItem t: tdItems) {
                    size +=t.fieldType.getLen();
            }


        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        List<TDItem> tdItems1 = td1.tdItems;
        List<TDItem> tdItems = td2.tdItems;
        List<TDItem> newTdItems = new ArrayList<>();

        for (TDItem t: tdItems1) {
            newTdItems.add(t);
        }
        for(TDItem t : tdItems) {
            newTdItems.add(t);
        }

        // some code goes here
        return new TupleDesc(newTdItems);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    @Override
    public boolean equals(Object o) {
        // some code goes here
        if(this.getClass().isInstance(o)){
            TupleDesc tupleDesc = (TupleDesc) o;
            if(numFields() == tupleDesc.numFields()){
                for (int i=0; i<numFields(); i++) {
                    if(!tdItems.get(i).fieldType.equals(tupleDesc.tdItems.get(i).fieldType)){
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }



    @Override
    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        return tdItems.hashCode();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    @Override
    public String toString() {
        // some code goes here
        return tdItems.toString();
    }
}

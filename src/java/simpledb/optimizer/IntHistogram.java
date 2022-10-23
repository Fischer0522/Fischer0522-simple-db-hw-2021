package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
        private int buckets;
        private int min;
        private int max;
        private double width;
        private int ntups;
        private int[] selectBuckets;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = buckets;
        this.max = max;
        this.min = min;
        this.width = (max - min + 1.0) / buckets;
        this.selectBuckets = new int[buckets];
        this.ntups = 0;

    }

    private int getIndex(int value) {
        return (int) ((value - min) / width);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */


    public void addValue(int v) {
        if (v < min || v > max) {
            throw new IllegalArgumentException("value is out of bound");
        }
        int index = getIndex(v);
        selectBuckets[index]++;
        ntups++;


        // some code goes here

    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        switch(op) {
            case LESS_THAN:
                if (v <= min) {
                    return 0.0;
                } else if (v >= max) {
                    return 1.0;
                } else {
                    int index = getIndex(v);
                    double tuple = 0;
                    for(int i = 0; i < index;i++) {
                        tuple += selectBuckets[i];
                    }
                    double bPart = (v - (min + index * width)) / width;
                    double bFraction = selectBuckets[index] * 1.0 ;
                    tuple += bPart * bFraction;
                    return tuple / ntups;
                }
            case LESS_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.LESS_THAN,v+1);
            case GREATER_THAN:
                return 1- estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ,v);
            case GREATER_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.GREATER_THAN,v-1);
            case EQUALS:
                return estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ,v)-estimateSelectivity(Predicate.Op.LESS_THAN,v);
            case NOT_EQUALS:
                return 1 - estimateSelectivity(Predicate.Op.EQUALS,v);
            default:
                throw new IllegalArgumentException("Operation is illegal");
        }

    	// some code goes here
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        int sum = 0;
        for( int b : selectBuckets) {
            sum += b;
        }
        if (sum == 0) {
            return 0;
        }
        return (1.0 * sum) / ntups;

    }

    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    @Override
    public String toString() {
        // some code goes here
        Map<Object,Object> map = new HashMap<>();
        for(int i = 0; i < buckets;i++) {
            map.put(i,selectBuckets[i]);
        }
        map.put("width",this.width);
        map.put("nums of tuples",this.ntups);
        return map.toString();
    }
}

package simpledb.systemtest;

import org.junit.Test;
import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.Type;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.TransactionId;

import java.io.File;

public class SimpleQueryTest {

    @Test
    public void queryTest(){
        Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
        String names[] = new String[]{ "field0", "field1", "field2" };
        TupleDesc descriptor = new TupleDesc(types, names);

        // create the table, associate it with some_data_file.dat
        // and tell the catalog about the schema of this table.
        HeapFile table1 = new HeapFile(new File("some_data_file.dat"), descriptor);



        System.out.println("111"+table1.getTupleDesc());

        Database.getCatalog().addTable(table1, "some_data_file");

        // construct the query: we use a simple SeqScan, which spoonfeeds
        // tuples via its iterator.
        TransactionId tid = new TransactionId();
        SeqScan f = new SeqScan(tid, table1.getId());



        try {
            // and run it
            f.open();

            while (f.hasNext()) {
                Tuple tup = f.next();
                System.out.println("ops"+tup);
            }
            f.close();
            Database.getBufferPool().transactionComplete(tid);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println ("Exception : " + e);
        }
    }


    }


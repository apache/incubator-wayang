package org.qcri.rheem.iejoin.operators.spark_helpers;

import org.apache.spark.api.java.function.Function2;
import org.qcri.rheem.core.util.Copyable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by khayyzy on 5/28/16.
 */
public class addUniqueID<Input extends Copyable<Input>> implements
        Function2<Integer, Iterator<Input>, Iterator<Tuple2<Long, Input>>> {

    int block;
    int start;

    public addUniqueID() {

        this.start = 0;
        this.block = 0;
    }

    public addUniqueID(int block, int start) {
        this.block = block;
        this.start = start;
    }

    @SuppressWarnings("unchecked")
    public Iterator<Tuple2<Long, Input>> call(Integer in, Iterator<Input> arg0)
            throws Exception {

        ArrayList<Tuple2<Long, Input>> outList = new ArrayList<Tuple2<Long, Input>>(300000);

        long tupleLocalID = (block * in) + start;
        // System.out.println("tupleLocalID = "+tupleLocalID);
        while (arg0.hasNext()) {

            Input t = arg0.next().copy();

            outList.add(new Tuple2<Long, Input>(tupleLocalID, t));
            tupleLocalID = tupleLocalID + 1;
        }
        return outList.iterator();
    }
}


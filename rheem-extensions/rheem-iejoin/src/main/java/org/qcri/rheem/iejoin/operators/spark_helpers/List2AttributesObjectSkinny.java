package org.qcri.rheem.iejoin.operators.spark_helpers;

import org.qcri.rheem.iejoin.data.Data;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Created by khayyzy on 5/28/16.
 */
public class List2AttributesObjectSkinny<Type0 extends Comparable<Type0>,
        Type1 extends Comparable<Type1>> implements Serializable {
    /**
     *
     */
    private static final long serialVersionUID = -7917020106773932879L;

    Data<Type0, Type1>[] list1;
    long partitionID;

    public List2AttributesObjectSkinny(Data[] list1, long partID) {
        this.list1 = list1;
        this.partitionID = partID;
    }

    public Tuple2<Type1, Type1> findMinMaxRank() {
        Type1 min = list1[0].getRank();
        Type1 max = list1[0].getRank();
        for (int i = 1; i < list1.length; i++) {
            Type1 rnk = list1[i].getRank();
            min = min.compareTo(rnk) < 0 ? min : rnk;
            max = max.compareTo(rnk) > 0 ? max : rnk;
        }
        return new Tuple2<Type1, Type1>(min, max);

    }

    public Type0 getHeadTupleValue() {
        return list1[0].getValue();
    }

    public Data<Type0, Type1> getHeadTupleData() {
        return list1[0];
    }

    public Data<Type0, Type1> getTailTupleData() {
        return list1[list1.length - 1];
    }

    public long getPartitionID() {
        return partitionID;
    }

    public Data<Type0, Type1>[] getList1() {
        return list1;
    }

    public boolean isEmpty() {

        return (list1.length == 0);
    }

    public String toString() {
        String output = "";
        for (int i = 0; i < list1.length; i++) {
            output = output + "(" + list1[i].toString() + "),";
        }
        return output;
    }
}

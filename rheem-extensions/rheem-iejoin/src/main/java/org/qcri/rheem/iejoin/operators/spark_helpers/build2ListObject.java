package org.qcri.rheem.iejoin.operators.spark_helpers;

import org.apache.spark.api.java.function.Function2;
import org.qcri.rheem.core.util.Copyable;
import org.qcri.rheem.iejoin.data.Data;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

/**
 * Created by khayyzy on 5/28/16.
 */
public class build2ListObject<TypeXPivot extends Comparable<TypeXPivot>, TypeXRef extends Comparable<TypeXRef>, Input extends Copyable<Input>>
        implements
        Function2<Integer, Iterator<Tuple2<Long, Input>>, Iterator<List2AttributesObjectSkinny<TypeXPivot, TypeXRef>>> {

    /**
     *
     */
    private static final long serialVersionUID = 6703700898776377115L;
    org.apache.spark.api.java.function.Function<Input, TypeXPivot> getXPivot;
    org.apache.spark.api.java.function.Function<Input, TypeXRef> getXRef;

    boolean list1ASC;
    boolean list1ASCSec;
    Integer ex1;
    Integer ex2;

    public build2ListObject(boolean list1ASC, boolean list1ASCSec, org.apache.spark.api.java.function.Function<Input, TypeXPivot> getXPivot, org.apache.spark.api.java.function.Function<Input, TypeXRef> getXRef) {
        this.list1ASC = list1ASC;
        this.list1ASCSec = list1ASCSec;
        this.getXPivot = getXPivot;
        this.getXRef = getXRef;
    }

    @SuppressWarnings("unchecked")
    public Iterator<List2AttributesObjectSkinny<TypeXPivot, TypeXRef>> call(Integer in,
                                                                            Iterator<Tuple2<Long, Input>> arg0) throws Exception {

        ArrayList<List2AttributesObjectSkinny<TypeXPivot, TypeXRef>> outList = new ArrayList<List2AttributesObjectSkinny<TypeXPivot, TypeXRef>>(1);

        ArrayList<Data<TypeXPivot, TypeXRef>> list1 = new ArrayList<Data<TypeXPivot, TypeXRef>>(300000);

        while (arg0.hasNext()) {

            Tuple2<Long, Input> t2 = arg0.next();
            Input t = t2._2().copy();

            list1.add(new Data(t2._1(), getXPivot.call(t), getXRef.call(t)));//(TypeXPivot) t.getField(getXPivot), (TypeXRef) t.getField(getXRef)));
        }
        Collections.sort(list1, new Data.Comparator(list1ASC, list1ASCSec));
        Data[] myData = new Data[list1.size()];
        list1.toArray(myData);
        List2AttributesObjectSkinny<TypeXPivot, TypeXRef> lo = new List2AttributesObjectSkinny<TypeXPivot, TypeXRef>(myData,
                in);
        if (!lo.isEmpty()) {
            outList.add(lo);
        }
        return outList.iterator();
    }
}


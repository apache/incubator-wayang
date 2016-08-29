package org.qcri.rheem.iejoin.operators.spark_helpers;

import org.apache.spark.api.java.function.Function;
import org.qcri.rheem.iejoin.data.Data;
import scala.Tuple2;

/**
 * Created by khayyzy on 5/28/16.
 */
public class extractData<TypeXPivot extends Comparable<TypeXPivot>,
        TypeXRef extends Comparable<TypeXRef>, Input> implements Function<Tuple2<Long, Input>, Data<TypeXPivot, TypeXRef>> {

    /**
     *
     */
    private static final long serialVersionUID = 3834945091845558509L;
    org.apache.spark.api.java.function.Function<Input, TypeXPivot> getXPivot;
    org.apache.spark.api.java.function.Function<Input, TypeXRef> getXRef;

    public extractData(org.apache.spark.api.java.function.Function<Input, TypeXPivot> getXPivot, org.apache.spark.api.java.function.Function<Input, TypeXRef> getXRef) {
        this.getXPivot = getXPivot;
        this.getXRef = getXRef;
    }

    public Data call(Tuple2<Long, Input> in) throws Exception {
        return new Data<TypeXPivot, TypeXRef>(in._1(),
                //(TypeXPivot) in._2().getField(getXPivot),
                //(TypeXRef) in._2().getField(getXRef));
                this.getXPivot.call(in._2()), this.getXRef.call(in._2()));
    }
}

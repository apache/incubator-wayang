package org.qcri.rheem.java.operators.subOperator;

import org.qcri.rheem.basic.data.Data;
import org.qcri.rheem.basic.data.Record;

import java.util.function.Function;

/**
 * Created by khayyzy on 5/28/16.
 */
public class extractData<TypeXPivot extends Comparable<TypeXPivot>,
        TypeXRef extends Comparable<TypeXRef>,Input> {

    /**
     *
     */
    private static final long serialVersionUID = 3834945091845558509L;
    Function<Input, TypeXPivot> getXPivot;
    Function<Input, TypeXRef> getXRef;

    public extractData(Function<Input, TypeXPivot> getXPivot,  Function<Input, TypeXRef> getXRef) {
        this.getXPivot = getXPivot;
        this.getXRef = getXRef;
    }

    public Data call(Input in) {
        return new Data<TypeXPivot, TypeXRef>(-1,
              //  (TypeXPivot) in.getField(getXPivot),
               // (TypeXRef) in.getField(getXRef));
                getXPivot.apply(in),
                getXRef.apply(in));
    }
}

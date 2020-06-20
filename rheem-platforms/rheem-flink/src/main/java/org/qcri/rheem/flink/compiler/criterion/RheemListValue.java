package org.qcri.rheem.flink.compiler.criterion;

import org.apache.flink.types.ListValue;

import java.util.Collection;

/**
 * Is a Wrapper for used in the criterion of the Loops
 */
public class RheemListValue extends ListValue<RheemValue> {

    public RheemListValue(Collection collection){
        super(collection);
    }

    public RheemListValue(){
        super();
    }


}

package org.qcri.rheem.flink.compiler.criterion;

import org.apache.flink.types.ListValue;
import org.apache.flink.types.Value;

import java.util.Collection;

/**
 * Created by bertty on 20-09-17.
 */
public class RheemListValue extends ListValue<RheemValue> {

    public RheemListValue(Collection collection){
        super(collection);
    }

    public RheemListValue(){
        super();
    }


}

package org.apache.wayang.flink.compiler.criterion;

import org.apache.flink.types.ListValue;

import java.util.Collection;

/**
 * Is a Wrapper for used in the criterion of the Loops
 */
public class WayangListValue extends ListValue<WayangValue> {

    public WayangListValue(Collection collection){
        super(collection);
    }

    public WayangListValue(){
        super();
    }


}

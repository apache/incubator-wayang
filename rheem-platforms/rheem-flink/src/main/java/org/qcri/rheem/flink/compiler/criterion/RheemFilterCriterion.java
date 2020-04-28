package org.qcri.rheem.flink.compiler.criterion;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.configuration.Configuration;

/**
 * Created by bertty on 21-09-17.
 */
public class RheemFilterCriterion<T> extends AbstractRichFunction implements FilterFunction<T> {

    private RheemAggregator rheemAggregator;
    private String name;


    public RheemFilterCriterion(String name){
        this.name = name;
    }

    @Override
    public void open(Configuration configuration){
        this.rheemAggregator = getIterationRuntimeContext().getIterationAggregator(this.name);
    }

    @Override
    public boolean filter(T t) throws Exception {
        this.rheemAggregator.aggregate(t);
        return true;
    }
}

package org.apache.incubator.wayang.flink.compiler.criterion;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.configuration.Configuration;

/**
 * Class create a {@Link FilterFunction} for use inside of the LoopOperators
 */
public class WayangFilterCriterion<T> extends AbstractRichFunction implements FilterFunction<T> {

    private WayangAggregator wayangAggregator;
    private String name;


    public WayangFilterCriterion(String name){
        this.name = name;
    }

    @Override
    public void open(Configuration configuration){
        this.wayangAggregator = getIterationRuntimeContext().getIterationAggregator(this.name);
    }

    @Override
    public boolean filter(T t) throws Exception {
        this.wayangAggregator.aggregate(t);
        return true;
    }
}

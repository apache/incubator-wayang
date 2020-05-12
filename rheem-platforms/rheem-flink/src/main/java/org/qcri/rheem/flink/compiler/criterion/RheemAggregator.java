package org.qcri.rheem.flink.compiler.criterion;

import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.types.ListValue;

import java.util.ArrayList;
import java.util.List;

/**
 * Class create a {@Link Aggregator} that generate aggregatorWrapper
 */
public class RheemAggregator implements Aggregator<ListValue<RheemValue>> {
    private List<RheemValue> elements;

    public RheemAggregator(){
        this.elements = new ArrayList<>();
    }

    @Override
    public ListValue<RheemValue> getAggregate() {
        return new RheemListValue(this.elements);
    }

    @Override
    public void aggregate(ListValue rheemValues) {
        this.elements.addAll(rheemValues);
    }


    public void aggregate(Object t){
        this.elements.add(new RheemValue(t));
    }

    @Override
    public void reset() {
        this.elements.clear();
    }
}

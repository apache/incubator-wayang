package org.apache.wayang.flink.compiler.criterion;

import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.types.ListValue;

import java.util.ArrayList;
import java.util.List;

/**
 * Class create a {@Link Aggregator} that generate aggregatorWrapper
 */
public class WayangAggregator implements Aggregator<ListValue<WayangValue>> {
    private List<WayangValue> elements;

    public WayangAggregator(){
        this.elements = new ArrayList<>();
    }

    @Override
    public ListValue<WayangValue> getAggregate() {
        return new WayangListValue(this.elements);
    }

    @Override
    public void aggregate(ListValue wayangValues) {
        this.elements.addAll(wayangValues);
    }


    public void aggregate(Object t){
        this.elements.add(new WayangValue(t));
    }

    @Override
    public void reset() {
        this.elements.clear();
    }
}

package org.components.aggregator;

import org.functions.AggregatorFunction;

import java.util.List;
import java.util.Map;

public class Aggregator {
    private final AggregatorFunction aggregator;

    public Aggregator(AggregatorFunction aggregator){
        this.aggregator = aggregator;
    }

    public Object aggregate(List<Object> ClientResponses, Map<String, Object> server_hyperparams){
        return aggregator.apply(ClientResponses, server_hyperparams);
    }
}

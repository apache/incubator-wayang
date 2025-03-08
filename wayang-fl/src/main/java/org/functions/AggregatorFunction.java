package org.functions;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@FunctionalInterface
public interface AggregatorFunction extends Serializable {
    Object apply(List<Object> ClientResponses, Map<String, Object> server_hyperparams);
}


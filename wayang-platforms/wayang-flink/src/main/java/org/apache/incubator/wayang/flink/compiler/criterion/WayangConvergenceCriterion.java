package org.apache.incubator.wayang.flink.compiler.criterion;


import org.apache.flink.api.common.aggregators.ConvergenceCriterion;
import org.apache.incubator.wayang.core.function.FunctionDescriptor;

import java.io.Serializable;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Class create a {@Link ConvergenceCriterion} that generate aggregatorWrapper
 */
public class WayangConvergenceCriterion<T>
        implements ConvergenceCriterion<WayangListValue>, Serializable {

    private boolean doWhile;
    private FunctionDescriptor.SerializablePredicate<Collection<T>> predicate;

    public WayangConvergenceCriterion(FunctionDescriptor.SerializablePredicate<Collection<T>> predicate){
        this.predicate = predicate;
        this.doWhile = false;
    }

    public WayangConvergenceCriterion setDoWhile(boolean doWhile){
        this.doWhile = doWhile;
        return this;
    }

    @Override
    public boolean isConverged(int i, WayangListValue tWayangValue) {
        if( i == 1 && this.doWhile ){
            return true;
        }
        Collection collection = tWayangValue
                .stream()
                .map(
                    wayangValue -> wayangValue.get()
                ).collect(
                    Collectors.toList()
        );
        return this.predicate.test(collection);
    }

}

package org.qcri.rheem.flink.compiler.criterion;


import org.apache.flink.api.common.aggregators.ConvergenceCriterion;
import org.qcri.rheem.core.function.FunctionDescriptor;

import java.io.Serializable;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Class create a {@Link ConvergenceCriterion} that generate aggregatorWrapper
 */
public class RheemConvergenceCriterion<T>
        implements ConvergenceCriterion<RheemListValue>, Serializable {

    private boolean doWhile;
    private FunctionDescriptor.SerializablePredicate<Collection<T>> predicate;

    public RheemConvergenceCriterion(FunctionDescriptor.SerializablePredicate<Collection<T>> predicate){
        this.predicate = predicate;
        this.doWhile = false;
    }

    public RheemConvergenceCriterion setDoWhile(boolean doWhile){
        this.doWhile = doWhile;
        return this;
    }

    @Override
    public boolean isConverged(int i, RheemListValue tRheemValue) {
        if( i == 1 && this.doWhile ){
            return true;
        }
        Collection collection = tRheemValue
                .stream()
                .map(
                    rheemValue -> rheemValue.get()
                ).collect(
                    Collectors.toList()
        );
        return this.predicate.test(collection);
    }

}

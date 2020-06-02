package org.qcri.rheem.flink.mapping;

import org.qcri.rheem.basic.operators.GroupByOperator;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.mapping.OperatorPattern;
import org.qcri.rheem.core.mapping.PlanTransformation;
import org.qcri.rheem.core.mapping.ReplacementSubplanFactory;
import org.qcri.rheem.core.mapping.SubplanPattern;
import org.qcri.rheem.flink.operators.FlinkGroupByOperator;
import org.qcri.rheem.flink.platform.FlinkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link GroupByOperator} to {@link FlinkGroupByOperator}.
 */
@SuppressWarnings("unchecked")
public class GroupByMapping implements Mapping{
    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                FlinkPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "groupby", new GroupByOperator<>((FunctionDescriptor.SerializableFunction)null, Void.class, Void.class), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }


    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<GroupByOperator>(
                (matchedOperator, epoch) -> new FlinkGroupByOperator<>(matchedOperator).at(epoch)
        );
    }
}

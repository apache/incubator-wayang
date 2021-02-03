package org.apache.wayang.flink.mapping;

import org.apache.wayang.basic.operators.GroupByOperator;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.mapping.OperatorPattern;
import org.apache.wayang.core.mapping.PlanTransformation;
import org.apache.wayang.core.mapping.ReplacementSubplanFactory;
import org.apache.wayang.core.mapping.SubplanPattern;
import org.apache.wayang.flink.operators.FlinkGroupByOperator;
import org.apache.wayang.flink.platform.FlinkPlatform;

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

package org.apache.wayang.flink.mapping;

import org.apache.wayang.basic.operators.DoWhileOperator;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.mapping.OperatorPattern;
import org.apache.wayang.core.mapping.PlanTransformation;
import org.apache.wayang.core.mapping.ReplacementSubplanFactory;
import org.apache.wayang.core.mapping.SubplanPattern;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.flink.operators.FlinkDoWhileOperator;
import org.apache.wayang.flink.platform.FlinkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link DoWhileOperator} to {@link FlinkDoWhileOperator}.
 */
@SuppressWarnings("unchecked")
public class DoWhileMapping implements Mapping {
    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(
                new PlanTransformation(
                        this.createSubplanPattern(),
                        this.createReplacementSubplanFactory(),
                        FlinkPlatform.getInstance()
                )
        );
    }

    @SuppressWarnings("unchecked")
    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "loop", new DoWhileOperator<>(DataSetType.none(), DataSetType.none(), (PredicateDescriptor) null, 1), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<DoWhileOperator<?, ?>>(
                (matchedOperator, epoch) -> new FlinkDoWhileOperator<>(matchedOperator).at(epoch)
        );
    }
}

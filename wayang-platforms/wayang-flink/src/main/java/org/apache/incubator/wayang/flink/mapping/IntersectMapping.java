package org.apache.incubator.wayang.flink.mapping;

import org.apache.incubator.wayang.basic.operators.IntersectOperator;
import org.apache.incubator.wayang.core.mapping.Mapping;
import org.apache.incubator.wayang.core.mapping.OperatorPattern;
import org.apache.incubator.wayang.core.mapping.PlanTransformation;
import org.apache.incubator.wayang.core.mapping.ReplacementSubplanFactory;
import org.apache.incubator.wayang.core.mapping.SubplanPattern;
import org.apache.incubator.wayang.core.types.DataSetType;
import org.apache.incubator.wayang.flink.operators.FlinkIntersectOperator;
import org.apache.incubator.wayang.flink.platform.FlinkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link IntersectOperator} to {@link FlinkIntersectOperator}.
 */
@SuppressWarnings("unchecked")
public class IntersectMapping implements Mapping{

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                FlinkPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern<>(
                "intersect", new IntersectOperator<>(DataSetType.none()), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<IntersectOperator>(
                (matchedOperator, epoch) -> new FlinkIntersectOperator<>(matchedOperator).at(epoch)
        );
    }
}

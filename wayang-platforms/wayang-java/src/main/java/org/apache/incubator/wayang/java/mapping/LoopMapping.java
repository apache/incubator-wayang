package org.apache.incubator.wayang.java.mapping;

import org.apache.incubator.wayang.basic.operators.LoopOperator;
import org.apache.incubator.wayang.core.function.PredicateDescriptor;
import org.apache.incubator.wayang.core.mapping.Mapping;
import org.apache.incubator.wayang.core.mapping.OperatorPattern;
import org.apache.incubator.wayang.core.mapping.PlanTransformation;
import org.apache.incubator.wayang.core.mapping.ReplacementSubplanFactory;
import org.apache.incubator.wayang.core.mapping.SubplanPattern;
import org.apache.incubator.wayang.core.types.DataSetType;
import org.apache.incubator.wayang.java.operators.JavaLoopOperator;
import org.apache.incubator.wayang.java.platform.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link LoopOperator} to {@link JavaLoopOperator}.
 */
@SuppressWarnings("unchecked")
public class LoopMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                JavaPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "loop", new LoopOperator<>(DataSetType.none(), DataSetType.none(), (PredicateDescriptor) null, 1), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<LoopOperator>(
                (matchedOperator, epoch) -> new JavaLoopOperator<>(matchedOperator).at(epoch)
        );
    }
}

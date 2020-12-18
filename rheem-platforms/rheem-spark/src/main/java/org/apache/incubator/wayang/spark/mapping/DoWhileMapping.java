package io.rheem.rheem.spark.mapping;

import io.rheem.rheem.basic.operators.DoWhileOperator;
import io.rheem.rheem.basic.operators.LoopOperator;
import io.rheem.rheem.core.function.PredicateDescriptor;
import io.rheem.rheem.core.mapping.Mapping;
import io.rheem.rheem.core.mapping.OperatorPattern;
import io.rheem.rheem.core.mapping.PlanTransformation;
import io.rheem.rheem.core.mapping.ReplacementSubplanFactory;
import io.rheem.rheem.core.mapping.SubplanPattern;
import io.rheem.rheem.core.types.DataSetType;
import io.rheem.rheem.spark.operators.SparkDoWhileOperator;
import io.rheem.rheem.spark.operators.SparkLoopOperator;
import io.rheem.rheem.spark.platform.SparkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link LoopOperator} to {@link SparkLoopOperator}.
 */
@SuppressWarnings("unchecked")
public class DoWhileMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                SparkPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "loop", new DoWhileOperator<>(DataSetType.none(), DataSetType.none(), (PredicateDescriptor) null, 1), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<DoWhileOperator>(
                (matchedOperator, epoch) -> new SparkDoWhileOperator<>(matchedOperator).at(epoch)
        );
    }
}

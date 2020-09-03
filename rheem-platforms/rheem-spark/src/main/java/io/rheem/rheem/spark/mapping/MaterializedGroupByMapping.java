package io.rheem.rheem.spark.mapping;

import io.rheem.rheem.basic.operators.MaterializedGroupByOperator;
import io.rheem.rheem.core.mapping.Mapping;
import io.rheem.rheem.core.mapping.OperatorPattern;
import io.rheem.rheem.core.mapping.PlanTransformation;
import io.rheem.rheem.core.mapping.ReplacementSubplanFactory;
import io.rheem.rheem.core.mapping.SubplanPattern;
import io.rheem.rheem.core.types.DataSetType;
import io.rheem.rheem.spark.operators.SparkMaterializedGroupByOperator;
import io.rheem.rheem.spark.platform.SparkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link MaterializedGroupByOperator} to {@link SparkMaterializedGroupByOperator}.
 */
public class MaterializedGroupByMapping implements Mapping {

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
                "operator", new MaterializedGroupByOperator<>(null, DataSetType.none(), DataSetType.groupedNone()), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<MaterializedGroupByOperator>(
                (matchedOperator, epoch) -> new SparkMaterializedGroupByOperator<>(matchedOperator).at(epoch)
        );
    }
}

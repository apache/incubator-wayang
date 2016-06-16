package org.qcri.rheem.spark.mapping;

import org.qcri.rheem.basic.operators.GlobalMaterializedGroupOperator;
import org.qcri.rheem.basic.operators.GlobalReduceOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.operators.SparkGlobalMaterializedGroupOperator;
import org.qcri.rheem.spark.operators.SparkGlobalReduceOperator;
import org.qcri.rheem.spark.platform.SparkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link GlobalMaterializedGroupOperator} to {@link SparkGlobalMaterializedGroupOperator}.
 */
@SuppressWarnings("unchecked")
public class GlobalMaterializedGroupToSparkGlobalMaterializedGroupMapping implements Mapping {

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
                "group", new GlobalMaterializedGroupOperator<>(DataSetType.none(), DataSetType.none()), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<GlobalMaterializedGroupOperator>(
                (matchedOperator, epoch) -> new SparkGlobalMaterializedGroupOperator<>(
                        matchedOperator.getInputType(),
                        matchedOperator.getOutputType()
                ).at(epoch)
        );
    }

}

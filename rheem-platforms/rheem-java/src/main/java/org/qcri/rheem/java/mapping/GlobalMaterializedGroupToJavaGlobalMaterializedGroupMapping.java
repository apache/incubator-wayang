package org.qcri.rheem.java.mapping;

import org.qcri.rheem.basic.operators.GlobalMaterializedGroupOperator;
import org.qcri.rheem.basic.operators.GlobalReduceOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.JavaPlatform;
import org.qcri.rheem.java.operators.JavaGlobalMaterializedGroupOperator;
import org.qcri.rheem.java.operators.JavaGlobalReduceOperator;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link GlobalReduceOperator} to {@link JavaGlobalReduceOperator}.
 */
@SuppressWarnings("unchecked")
public class GlobalMaterializedGroupToJavaGlobalMaterializedGroupMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(
                new PlanTransformation(
                        this.createSubplanPattern(),
                        this.createReplacementSubplanFactory(),
                        JavaPlatform.getInstance()
                )
        );
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "group", new GlobalMaterializedGroupOperator<>(null, DataSetType.none()), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<GlobalMaterializedGroupOperator>(
                (matchedOperator, epoch) -> new JavaGlobalMaterializedGroupOperator<>(
                        matchedOperator.getInputType(),
                        matchedOperator.getOutputType()
                ).at(epoch)
        );
    }
}

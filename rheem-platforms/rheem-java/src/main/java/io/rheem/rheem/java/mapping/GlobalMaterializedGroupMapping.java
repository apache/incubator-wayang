package io.rheem.rheem.java.mapping;

import io.rheem.rheem.basic.operators.GlobalMaterializedGroupOperator;
import io.rheem.rheem.core.mapping.Mapping;
import io.rheem.rheem.core.mapping.OperatorPattern;
import io.rheem.rheem.core.mapping.PlanTransformation;
import io.rheem.rheem.core.mapping.ReplacementSubplanFactory;
import io.rheem.rheem.core.mapping.SubplanPattern;
import io.rheem.rheem.core.types.DataSetType;
import io.rheem.rheem.java.operators.JavaGlobalMaterializedGroupOperator;
import io.rheem.rheem.java.platform.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link GlobalMaterializedGroupOperator} to {@link JavaGlobalMaterializedGroupOperator}.
 */
@SuppressWarnings("unchecked")
public class GlobalMaterializedGroupMapping implements Mapping {

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
                "group", new GlobalMaterializedGroupOperator<>(DataSetType.none(), DataSetType.groupedNone()), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<GlobalMaterializedGroupOperator>(
                (matchedOperator, epoch) -> new JavaGlobalMaterializedGroupOperator<>(matchedOperator).at(epoch)
        );
    }
}

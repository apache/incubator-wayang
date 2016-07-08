package org.qcri.rheem.postgres.mapping;

import org.qcri.rheem.basic.function.ProjectionDescriptor;
import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.basic.operators.ProjectionOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.postgres.PostgresPlatform;
import org.qcri.rheem.postgres.operators.PostgresProjectionOperator;

import java.util.Collection;
import java.util.Collections;

/**

/**
 * Mapping from {@link ProjectionOperator} to {@link PostgresProjectionOperator}.
 */
@SuppressWarnings("unchecked")
public class PostgresProjectionMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                PostgresPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "projection", new ProjectionOperator<>((ProjectionDescriptor) null, null, null), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<ProjectionOperator>(
                (matchedOperator, epoch) -> new PostgresProjectionOperator<>(matchedOperator).at(epoch)
        );
    }
}

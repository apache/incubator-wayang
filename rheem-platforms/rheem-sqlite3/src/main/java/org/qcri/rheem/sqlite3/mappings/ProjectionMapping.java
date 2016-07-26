package org.qcri.rheem.sqlite3.mappings;

import org.qcri.rheem.basic.operators.ProjectionOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.sqlite3.Sqlite3Platform;
import org.qcri.rheem.sqlite3.operators.Sqlite3ProjectionOperator;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link ProjectionOperator} to {@link Sqlite3ProjectionOperator}.
 */
@SuppressWarnings("unchecked")
public class ProjectionMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(
                new PlanTransformation(
                        this.createSubplanPattern(),
                        this.createReplacementSubplanFactory(),
                        Sqlite3Platform.getInstance()
                )
        );
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "projection", new ProjectionOperator<>(null, DataSetType.none(), DataSetType.none()), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<ProjectionOperator<?, ?>>(
                (matchedOperator, epoch) -> new Sqlite3ProjectionOperator<>(matchedOperator).at(epoch)
        );
    }
}

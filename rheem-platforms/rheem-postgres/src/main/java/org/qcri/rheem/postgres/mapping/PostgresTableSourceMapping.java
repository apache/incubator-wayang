package org.qcri.rheem.postgres.mapping;

import org.qcri.rheem.basic.operators.TableSource;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.postgres.PostgresPlatform;
import org.qcri.rheem.postgres.operators.PostgresTableSource;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link TableSource} to {@link PostgresTableSource}.
 */
public class PostgresTableSourceMapping implements Mapping {

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
                "source", new TableSource(null, DataSetType.none()), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<TableSource>(
                (matchedOperator, epoch) -> new PostgresTableSource<>(matchedOperator).at(epoch)
        );
    }
}

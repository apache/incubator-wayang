package org.qcri.rheem.sqlite3.mappings;

import org.qcri.rheem.basic.operators.TableSource;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.sqlite3.Sqlite3Platform;
import org.qcri.rheem.sqlite3.operators.Sqlite3TableSource;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link TableSource} to {@link Sqlite3TableSource}.
 */
@SuppressWarnings("unchecked")
public class TableSourceMapping implements Mapping {

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
                "table", new TableSource<>("", DataSetType.none()), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<TableSource<?>>(
                (matchedOperator, epoch) -> new Sqlite3TableSource<>(matchedOperator).at(epoch)
        );
    }
}

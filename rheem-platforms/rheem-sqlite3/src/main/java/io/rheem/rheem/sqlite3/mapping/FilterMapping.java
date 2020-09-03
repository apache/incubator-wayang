package io.rheem.rheem.sqlite3.mapping;

import io.rheem.rheem.basic.data.Record;
import io.rheem.rheem.basic.operators.FilterOperator;
import io.rheem.rheem.core.mapping.Mapping;
import io.rheem.rheem.core.mapping.OperatorPattern;
import io.rheem.rheem.core.mapping.PlanTransformation;
import io.rheem.rheem.core.mapping.ReplacementSubplanFactory;
import io.rheem.rheem.core.mapping.SubplanPattern;
import io.rheem.rheem.sqlite3.operators.Sqlite3FilterOperator;
import io.rheem.rheem.sqlite3.platform.Sqlite3Platform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link FilterOperator} to {@link Sqlite3FilterOperator}.
 */
@SuppressWarnings("unchecked")
public class FilterMapping implements Mapping {

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
        final OperatorPattern operatorPattern = new OperatorPattern<>(
                "filter", new FilterOperator<>(null, Record.class), false
        ).withAdditionalTest(op -> op.getPredicateDescriptor().getSqlImplementation() != null);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<FilterOperator<Record>>(
                (matchedOperator, epoch) -> new Sqlite3FilterOperator(matchedOperator).at(epoch)
        );
    }
}

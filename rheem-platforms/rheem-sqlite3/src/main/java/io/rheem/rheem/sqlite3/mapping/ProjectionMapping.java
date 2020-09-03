package io.rheem.rheem.sqlite3.mapping;

import io.rheem.rheem.basic.data.Record;
import io.rheem.rheem.basic.function.ProjectionDescriptor;
import io.rheem.rheem.basic.operators.MapOperator;
import io.rheem.rheem.core.mapping.Mapping;
import io.rheem.rheem.core.mapping.OperatorPattern;
import io.rheem.rheem.core.mapping.PlanTransformation;
import io.rheem.rheem.core.mapping.ReplacementSubplanFactory;
import io.rheem.rheem.core.mapping.SubplanPattern;
import io.rheem.rheem.sqlite3.operators.Sqlite3ProjectionOperator;
import io.rheem.rheem.sqlite3.platform.Sqlite3Platform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link MapOperator} to {@link Sqlite3ProjectionOperator}.
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
        final OperatorPattern<MapOperator<Record, Record>> operatorPattern = new OperatorPattern<>(
                "projection", new MapOperator<>(null, Record.class, Record.class), false
        )
                .withAdditionalTest(op -> op.getFunctionDescriptor() instanceof ProjectionDescriptor)
                .withAdditionalTest(op -> op.getNumInputs() == 1); // No broadcasts.
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<MapOperator<Record, Record>>(
                (matchedOperator, epoch) -> new Sqlite3ProjectionOperator(matchedOperator).at(epoch)
        );
    }
}

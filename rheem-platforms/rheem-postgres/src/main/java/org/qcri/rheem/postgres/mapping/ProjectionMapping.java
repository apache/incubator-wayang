package org.qcri.rheem.postgres.mapping;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.function.ProjectionDescriptor;
import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.mapping.OperatorPattern;
import org.qcri.rheem.core.mapping.PlanTransformation;
import org.qcri.rheem.core.mapping.ReplacementSubplanFactory;
import org.qcri.rheem.core.mapping.SubplanPattern;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.postgres.operators.PostgresProjectionOperator;
import org.qcri.rheem.postgres.platform.PostgresPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * /**
 * Mapping from {@link MapOperator} to {@link PostgresProjectionOperator}.
 */
@SuppressWarnings("unchecked")
public class ProjectionMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                PostgresPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        OperatorPattern<MapOperator<Record, Record>> operatorPattern = new OperatorPattern<>(
                "projection",
                new MapOperator<>(
                        null,
                        DataSetType.createDefault(Record.class),
                        DataSetType.createDefault(Record.class)
                ),
                false
        )
                .withAdditionalTest(op -> op.getFunctionDescriptor() instanceof ProjectionDescriptor)
                .withAdditionalTest(op -> op.getNumInputs() == 1); // No broadcasts.
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<MapOperator<Record, Record>>(
                (matchedOperator, epoch) -> new PostgresProjectionOperator(matchedOperator).at(epoch)
        );
    }
}

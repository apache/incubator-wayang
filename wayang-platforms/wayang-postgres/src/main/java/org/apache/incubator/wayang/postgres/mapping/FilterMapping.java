package org.apache.incubator.wayang.postgres.mapping;

import org.apache.incubator.wayang.basic.data.Record;
import org.apache.incubator.wayang.basic.operators.FilterOperator;
import org.apache.incubator.wayang.core.mapping.Mapping;
import org.apache.incubator.wayang.core.mapping.OperatorPattern;
import org.apache.incubator.wayang.core.mapping.PlanTransformation;
import org.apache.incubator.wayang.core.mapping.ReplacementSubplanFactory;
import org.apache.incubator.wayang.core.mapping.SubplanPattern;
import org.apache.incubator.wayang.core.types.DataSetType;
import org.apache.incubator.wayang.postgres.operators.PostgresFilterOperator;
import org.apache.incubator.wayang.postgres.platform.PostgresPlatform;

import java.util.Collection;
import java.util.Collections;


/**
 * Mapping from {@link FilterOperator} to {@link PostgresFilterOperator}.
 */
@SuppressWarnings("unchecked")
public class FilterMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                PostgresPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern<FilterOperator<Record>> operatorPattern = new OperatorPattern<>(
                "filter", new FilterOperator<>(null, DataSetType.createDefault(Record.class)), false
        ).withAdditionalTest(op -> op.getPredicateDescriptor().getSqlImplementation() != null);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<FilterOperator>(
                (matchedOperator, epoch) -> new PostgresFilterOperator(matchedOperator).at(epoch)
        );
    }
}

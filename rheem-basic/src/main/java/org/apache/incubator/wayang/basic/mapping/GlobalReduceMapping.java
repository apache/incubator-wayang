package io.rheem.rheem.basic.mapping;

import io.rheem.rheem.basic.operators.GlobalReduceOperator;
import io.rheem.rheem.basic.operators.GroupByOperator;
import io.rheem.rheem.basic.operators.ReduceByOperator;
import io.rheem.rheem.basic.operators.ReduceOperator;
import io.rheem.rheem.core.mapping.Mapping;
import io.rheem.rheem.core.mapping.OperatorPattern;
import io.rheem.rheem.core.mapping.PlanTransformation;
import io.rheem.rheem.core.mapping.ReplacementSubplanFactory;
import io.rheem.rheem.core.mapping.SubplanMatch;
import io.rheem.rheem.core.mapping.SubplanPattern;
import io.rheem.rheem.core.plan.rheemplan.Operator;
import io.rheem.rheem.core.types.DataSetType;

import java.util.Collection;
import java.util.Collections;

/**
 * This mapping detects combinations of the {@link GroupByOperator} and {@link ReduceOperator} and merges them into
 * a single {@link ReduceByOperator}.
 */
public class GlobalReduceMapping implements Mapping {


    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(this.createSubplanPattern(), new ReplacementFactory()));
    }

    @SuppressWarnings("unchecked")
    private SubplanPattern createSubplanPattern() {
        final OperatorPattern reducePattern = new OperatorPattern(
                "reduce",
                new ReduceOperator<>(
                        null,
                        DataSetType.none(),
                        DataSetType.none()
                ),
                false);
        return SubplanPattern.createSingleton(reducePattern);
    }

    private static class ReplacementFactory extends ReplacementSubplanFactory {

        @Override
        @SuppressWarnings("unchecked")
        protected Operator translate(SubplanMatch subplanMatch, int epoch) {
            final ReduceOperator reduce = (ReduceOperator) subplanMatch.getMatch("reduce").getOperator();

            return new GlobalReduceOperator<>(
                    reduce.getReduceDescriptor(), reduce.getInputType()
            ).at(epoch);
        }
    }


}

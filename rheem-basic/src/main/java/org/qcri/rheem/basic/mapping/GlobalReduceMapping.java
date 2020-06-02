package org.qcri.rheem.basic.mapping;

import org.qcri.rheem.basic.operators.GlobalReduceOperator;
import org.qcri.rheem.basic.operators.GroupByOperator;
import org.qcri.rheem.basic.operators.ReduceByOperator;
import org.qcri.rheem.basic.operators.ReduceOperator;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.mapping.OperatorPattern;
import org.qcri.rheem.core.mapping.PlanTransformation;
import org.qcri.rheem.core.mapping.ReplacementSubplanFactory;
import org.qcri.rheem.core.mapping.SubplanMatch;
import org.qcri.rheem.core.mapping.SubplanPattern;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.types.DataSetType;

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

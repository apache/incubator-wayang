package org.apache.incubator.wayang.basic.mapping;

import org.apache.incubator.wayang.basic.operators.GroupByOperator;
import org.apache.incubator.wayang.basic.operators.ReduceByOperator;
import org.apache.incubator.wayang.basic.operators.ReduceOperator;
import org.apache.incubator.wayang.core.mapping.Mapping;
import org.apache.incubator.wayang.core.mapping.OperatorPattern;
import org.apache.incubator.wayang.core.mapping.PlanTransformation;
import org.apache.incubator.wayang.core.mapping.ReplacementSubplanFactory;
import org.apache.incubator.wayang.core.mapping.SubplanMatch;
import org.apache.incubator.wayang.core.mapping.SubplanPattern;
import org.apache.incubator.wayang.core.plan.wayangplan.Operator;
import org.apache.incubator.wayang.core.types.DataSetType;

import java.util.Collection;
import java.util.Collections;

/**
 * This mapping detects combinations of the {@link GroupByOperator} and {@link ReduceOperator} and merges them into
 * a single {@link ReduceByOperator}.
 */
public class ReduceByMapping implements Mapping {


    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(this.createSubplanPattern(), new ReplacementFactory()));
    }

    @SuppressWarnings("unchecked")
    private SubplanPattern createSubplanPattern() {
        final OperatorPattern groupByPattern = new OperatorPattern(
                "groupBy",
                new GroupByOperator<>(
                        null,
                        DataSetType.none(),
                        DataSetType.groupedNone()
                ),
                false);
        final OperatorPattern reducePattern = new OperatorPattern(
                "reduce",
                ReduceOperator.createGroupedReduce(
                        null,
                        DataSetType.groupedNone(),
                        DataSetType.none()
                ),
                false);
        groupByPattern.connectTo(0, reducePattern, 0);
        return SubplanPattern.fromOperatorPatterns(groupByPattern, reducePattern);
    }

    private static class ReplacementFactory extends ReplacementSubplanFactory {

        @Override
        @SuppressWarnings("unchecked")
        protected Operator translate(SubplanMatch subplanMatch, int epoch) {
            final GroupByOperator groupBy = (GroupByOperator) subplanMatch.getMatch("groupBy").getOperator();
            final ReduceOperator reduce = (ReduceOperator) subplanMatch.getMatch("reduce").getOperator();

            return new ReduceByOperator<>(
                    groupBy.getKeyDescriptor(), reduce.getReduceDescriptor(), groupBy.getInputType()
            ).at(epoch);
        }
    }


}

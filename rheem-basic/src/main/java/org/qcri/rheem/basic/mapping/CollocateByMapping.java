package org.qcri.rheem.basic.mapping;

import org.qcri.rheem.basic.operators.CollocateByOperator;
import org.qcri.rheem.basic.operators.GroupByOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.plan.Operator;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Collection;
import java.util.Collections;

/**
 * This mapping translates the {@link GroupByOperator} into the {@link CollocateByOperator}.
 */
public class CollocateByMapping implements Mapping {


    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(createSubplanPattern(), new ReplacementFactory()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern groupByPattern = new OperatorPattern(
                "groupBy",
                new GroupByOperator<>(
                        null,
                        DataSetType.createDefault(Void.class),
                        DataSetType.createGrouped(Void.class)),
                false);
        return SubplanPattern.createSingleton(groupByPattern);
    }

    private static class ReplacementFactory extends ReplacementSubplanFactory {

        @Override
        protected Operator translate(SubplanMatch subplanMatch) {
            final GroupByOperator groupBy = (GroupByOperator) subplanMatch.getMatch("groupBy").getOperator();

            return new CollocateByOperator<>(
                    groupBy.getInputType(),
                    groupBy.getKeyDescriptor());
        }
    }


}

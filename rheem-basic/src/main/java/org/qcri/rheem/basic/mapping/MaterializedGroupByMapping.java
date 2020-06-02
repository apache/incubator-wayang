package org.qcri.rheem.basic.mapping;

import org.qcri.rheem.basic.operators.GroupByOperator;
import org.qcri.rheem.basic.operators.MaterializedGroupByOperator;
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
 * This mapping translates the {@link GroupByOperator} into the {@link MaterializedGroupByOperator}.
 */
public class MaterializedGroupByMapping implements Mapping {


    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(this.createSubplanPattern(), new ReplacementFactory()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern groupByPattern = new OperatorPattern<>(
                "groupBy",
                new GroupByOperator<>(
                        null,
                        DataSetType.none(),
                        DataSetType.groupedNone()
                ),
                false);
        return SubplanPattern.createSingleton(groupByPattern);
    }

    private static class ReplacementFactory extends ReplacementSubplanFactory {

        @Override
        protected Operator translate(SubplanMatch subplanMatch, int epoch) {
            final GroupByOperator groupBy = (GroupByOperator) subplanMatch.getMatch("groupBy").getOperator();

            return new MaterializedGroupByOperator<>(
                    groupBy.getKeyDescriptor(),
                    groupBy.getInputType(),
                    groupBy.getOutputType()
            ).at(epoch);
        }
    }


}

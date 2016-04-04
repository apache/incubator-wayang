package org.qcri.rheem.postgres.mapping;

import org.qcri.rheem.basic.operators.TableSource;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.postgres.operators.PostgresTableSource;
import org.qcri.rheem.postgres.PostgresPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link TableSource} to {@link PostgresTableSource}.
 */
public class PostgresTableSourceMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(this.createSubplanPattern(), new ReplacementFactory(),
                PostgresPlatform.getInstance()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "source", new org.qcri.rheem.basic.operators.TableSource(null), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private static class ReplacementFactory extends ReplacementSubplanFactory {

        @Override
        protected Operator translate(SubplanMatch subplanMatch, int epoch) {
            final TableSource originalSource = (TableSource) subplanMatch.getMatch("source").getOperator();
            return new PostgresTableSource(originalSource.getTableName()).at(epoch);
        }
    }
}

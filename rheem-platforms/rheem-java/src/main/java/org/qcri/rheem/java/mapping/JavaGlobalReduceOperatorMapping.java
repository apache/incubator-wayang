package org.qcri.rheem.java.mapping;

import org.qcri.rheem.basic.operators.GlobalReduceOperator;
import org.qcri.rheem.basic.operators.ReduceByOperator;
import org.qcri.rheem.basic.operators.TextFileSource;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.plan.Operator;
import org.qcri.rheem.java.operators.JavaGlobalReduceOperator;
import org.qcri.rheem.java.operators.JavaReduceByOperator;
import org.qcri.rheem.java.operators.JavaTextFileSource;
import org.qcri.rheem.java.plugin.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link GlobalReduceOperator} to {@link JavaGlobalReduceOperator}.
 * todo
 */
public class JavaGlobalReduceOperatorMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(createSubplanPattern(), new ReplacementFactory(),
                JavaPlatform.getInstance()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "reduce", new GlobalReduceOperator<>(null, null), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private static class ReplacementFactory extends ReplacementSubplanFactory {

        @Override
        protected Operator translate(SubplanMatch subplanMatch, int epoch) {
            final GlobalReduceOperator<?> originalOperator = (GlobalReduceOperator<?>) subplanMatch.getMatch("reduce").getOperator();
            return new JavaGlobalReduceOperator<>(
                    originalOperator.getType().unchecked(),
                    originalOperator.getReduceDescriptor().unchecked()
            ).at(epoch);
        }
    }
}

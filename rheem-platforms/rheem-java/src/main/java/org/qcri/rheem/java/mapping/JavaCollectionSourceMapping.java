package org.qcri.rheem.java.mapping;

import org.qcri.rheem.basic.operators.CollectionSource;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.java.operators.JavaCollectionSource;
import org.qcri.rheem.java.plugin.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link CollectionSource} to {@link JavaCollectionSource}.
 */
public class JavaCollectionSourceMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(createSubplanPattern(), new ReplacementFactory(),
                JavaPlatform.getInstance()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "source", new CollectionSource(Collections.emptyList(), null), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private static class ReplacementFactory extends ReplacementSubplanFactory {

        @Override
        protected Operator translate(SubplanMatch subplanMatch, int epoch) {
            final CollectionSource originalSource = (CollectionSource) subplanMatch.getMatch("source").getOperator();
            return new JavaCollectionSource(originalSource.getCollection(), originalSource.getOutput().getType()).at(epoch);
        }
    }
}

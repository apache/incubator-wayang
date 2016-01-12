package org.qcri.rheem.java.mapping;

import org.qcri.rheem.basic.operators.CollectionSource;
import org.qcri.rheem.basic.operators.TextFileSource;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.plan.Operator;
import org.qcri.rheem.java.operators.JavaCollectionSource;
import org.qcri.rheem.java.operators.JavaTextFileSource;

import java.text.CollationElementIterator;
import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link CollectionSource} to {@link JavaCollectionSource}.
 */
public class JavaCollectionSourceMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(createSubplanPattern(), new ReplacementFactory()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "source", new CollectionSource(Collections.emptyList(), null), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private static class ReplacementFactory extends ReplacementSubplanFactory {

        @Override
        protected Operator translate(SubplanMatch subplanMatch) {
            final CollectionSource originalSource = (CollectionSource) subplanMatch.getMatch("source").getOperator();
            return new JavaCollectionSource(originalSource.getCollection(), originalSource.getOutput().getType());
        }
    }
}

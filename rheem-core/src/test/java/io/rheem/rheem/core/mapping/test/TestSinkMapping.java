package io.rheem.rheem.core.mapping.test;

import io.rheem.rheem.core.mapping.Mapping;
import io.rheem.rheem.core.mapping.OperatorPattern;
import io.rheem.rheem.core.mapping.PlanTransformation;
import io.rheem.rheem.core.mapping.ReplacementSubplanFactory;
import io.rheem.rheem.core.mapping.SubplanPattern;
import io.rheem.rheem.core.plan.rheemplan.test.TestSink;
import io.rheem.rheem.core.plan.rheemplan.test.TestSink2;
import io.rheem.rheem.core.test.DummyPlatform;
import io.rheem.rheem.core.types.DataSetType;

import java.util.Collection;
import java.util.Collections;

/**
 * Dummy {@link Mapping} implementation from {@link TestSink} to {@link TestSink2}.
 */
public class TestSinkMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                DummyPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern<TestSink<?>> operatorPattern = new OperatorPattern<>(
                "sink", new TestSink<>(DataSetType.none()), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }


    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<TestSink<?>>(
                (matchedOperator, epoch) -> new TestSink2<>(matchedOperator.getType()).at(epoch)
        );
    }

    @Override
    public int hashCode() {
        return this.getClass().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj != null && this.getClass().equals(obj.getClass());
    }
}

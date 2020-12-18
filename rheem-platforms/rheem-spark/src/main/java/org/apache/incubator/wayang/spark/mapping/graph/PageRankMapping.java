package io.rheem.rheem.spark.mapping.graph;

import io.rheem.rheem.basic.operators.PageRankOperator;
import io.rheem.rheem.core.api.exception.RheemException;
import io.rheem.rheem.core.mapping.Mapping;
import io.rheem.rheem.core.mapping.OperatorPattern;
import io.rheem.rheem.core.mapping.PlanTransformation;
import io.rheem.rheem.core.mapping.ReplacementSubplanFactory;
import io.rheem.rheem.core.mapping.SubplanPattern;
import io.rheem.rheem.core.plan.rheemplan.Operator;
import io.rheem.rheem.spark.platform.SparkPlatform;

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link PageRankOperator} to io.rheem.rheem.spark.operators.graph.SparkPageRankOperator .
 */
public class PageRankMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                SparkPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern<>(
                "pageRank", new PageRankOperator(1), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<PageRankOperator>(
                (matchedOperator, epoch) -> {
                    // We need to instantiate the SparkPageRankOperator via reflection, because the Scala code will
                    // be compiled only after the Java code, which might cause compile errors.
                    try {
                        final Class<?> cls = Class.forName("io.rheem.rheem.spark.operators.graph.SparkPageRankOperator");
                        final Constructor<?> constructor = cls.getConstructor(PageRankOperator.class);
                        return (Operator) constructor.newInstance(matchedOperator);
                    } catch (Exception e) {
                        throw new RheemException(String.format("Could not apply %s.", this), e);
                    }
                }
        );
    }
}

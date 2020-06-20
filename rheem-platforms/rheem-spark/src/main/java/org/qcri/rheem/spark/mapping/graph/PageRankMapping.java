package org.qcri.rheem.spark.mapping.graph;

import org.qcri.rheem.basic.operators.PageRankOperator;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.mapping.OperatorPattern;
import org.qcri.rheem.core.mapping.PlanTransformation;
import org.qcri.rheem.core.mapping.ReplacementSubplanFactory;
import org.qcri.rheem.core.mapping.SubplanPattern;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.spark.platform.SparkPlatform;

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link PageRankOperator} to org.qcri.rheem.spark.operators.graph.SparkPageRankOperator .
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
                        final Class<?> cls = Class.forName("org.qcri.rheem.spark.operators.graph.SparkPageRankOperator");
                        final Constructor<?> constructor = cls.getConstructor(PageRankOperator.class);
                        return (Operator) constructor.newInstance(matchedOperator);
                    } catch (Exception e) {
                        throw new RheemException(String.format("Could not apply %s.", this), e);
                    }
                }
        );
    }
}

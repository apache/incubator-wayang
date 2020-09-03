package io.rheem.rheem.graphchi.mappings;

import io.rheem.rheem.basic.operators.PageRankOperator;
import io.rheem.rheem.core.mapping.Mapping;
import io.rheem.rheem.core.mapping.OperatorPattern;
import io.rheem.rheem.core.mapping.PlanTransformation;
import io.rheem.rheem.core.mapping.ReplacementSubplanFactory;
import io.rheem.rheem.core.mapping.SubplanPattern;
import io.rheem.rheem.graphchi.operators.GraphChiPageRankOperator;
import io.rheem.rheem.graphchi.platform.GraphChiPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Maps {@link PageRankOperator}s to {@link GraphChiPageRankOperator}s.
 */
public class PageRankMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(
                new PlanTransformation(
                        this.createSubplanPattern(),
                        this.createReplacementSubplanFactory(),
                        GraphChiPlatform.getInstance()
                )
        );
    }

    @SuppressWarnings("unchecked")
    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "pageRank", new PageRankOperator(1), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<PageRankOperator>(
                (matchedOperator, epoch) -> new GraphChiPageRankOperator(matchedOperator).at(epoch)
        );
    }

}

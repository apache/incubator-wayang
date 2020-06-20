package org.qcri.rheem.graphchi.mappings;

import org.qcri.rheem.basic.operators.PageRankOperator;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.mapping.OperatorPattern;
import org.qcri.rheem.core.mapping.PlanTransformation;
import org.qcri.rheem.core.mapping.ReplacementSubplanFactory;
import org.qcri.rheem.core.mapping.SubplanPattern;
import org.qcri.rheem.graphchi.operators.GraphChiPageRankOperator;
import org.qcri.rheem.graphchi.platform.GraphChiPlatform;

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

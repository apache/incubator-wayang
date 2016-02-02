package org.qcri.rheem.core.optimizer.costs;

import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.plan.Operator;
import org.qcri.rheem.core.plan.OutputSlot;
import org.qcri.rheem.core.plan.PhysicalPlan;

import java.util.Collection;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Mock-up for a repository that manages {@link CardinalityEstimator}s and the like.
 */
public class CardinalityEstimatorManager {

    private final TObjectDoubleMap<Class<? extends Predicate>> predicateSelectivities =
            new TObjectDoubleHashMap<>(32, 0.75f, -1d);
    private final TObjectDoubleMap<TransformationDescriptor<?, ? extends Stream<?>>> multimapSelectivities =
            new TObjectDoubleHashMap<>(32, 0.75f, -1d);

    public void registerSelectivity(Class<? extends Predicate> predicateClass, double selectivity) {
        Validate.notNull(predicateClass);
        Validate.inclusiveBetween(0d, 1d, selectivity);
        this.predicateSelectivities.put(predicateClass, selectivity);
    }

    public OptionalDouble getSelectivity(Class<? extends Predicate> predicateClass) {
        Validate.notNull(predicateClass);

        final double selectivity = this.predicateSelectivities.get(predicateClass);
        return selectivity == this.predicateSelectivities.getNoEntryValue() ? OptionalDouble.empty() : OptionalDouble.of(selectivity);
    }

    public void registerSelectivity(TransformationDescriptor<?, ? extends Stream<?>> transformationDescriptor, double selectivity) {
        Validate.notNull(transformationDescriptor);
        Validate.isTrue(selectivity >= 0d);
        this.multimapSelectivities.put(transformationDescriptor, selectivity);
    }

    public OptionalDouble getSelectivity(TransformationDescriptor<?, ? extends Stream<?>> transformationDescriptor) {
        Validate.notNull(transformationDescriptor);

        final double selectivity = this.multimapSelectivities.get(transformationDescriptor);
        return selectivity == this.multimapSelectivities.getNoEntryValue() ? OptionalDouble.empty() : OptionalDouble.of(selectivity);
    }

    public Map<OutputSlot<?>, CardinalityEstimate> estimateAllCardinatilities(PhysicalPlan physicalPlan) {
        final Collection<Operator> sources = physicalPlan.collectReachableTopLevelSources();
        // TODO
        return null;
    }

}

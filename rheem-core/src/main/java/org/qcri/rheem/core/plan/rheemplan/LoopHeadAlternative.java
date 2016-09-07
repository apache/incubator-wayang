package org.qcri.rheem.core.plan.rheemplan;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityPusher;
import org.qcri.rheem.core.optimizer.cardinality.LoopHeadAlternativeCardinalityPusher;
import org.qcri.rheem.core.util.RheemCollections;

import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Special {@link OperatorAlternative} for {@link LoopHeadOperator}s.
 */
public class LoopHeadAlternative extends OperatorAlternative implements LoopHeadOperator {

    private final LoopHeadOperator originalLoopHead;

    private Collection<OutputSlot<?>> loopBodyOutputs, finalLoopOutputs;

    private Collection<InputSlot<?>> loopBodyInputs, initializationInputs;

    /**
     * Creates a new instance.
     *
     * @param loopHead original {@link LoopHeadOperator} to be wrapped
     */
    LoopHeadAlternative(LoopHeadOperator loopHead) {
        super(loopHead);
        this.originalLoopHead = loopHead;
    }

    @Override
    public Alternative addAlternative(Operator alternativeOperator) {
        assert alternativeOperator.isLoopHead();
        assert !this.getAlternatives().isEmpty() || alternativeOperator == this.originalLoopHead;
        Alternative alternative = super.addAlternative(alternativeOperator);
        if (this.getAlternatives().size() == 1) {
            final Alternative originalAlternative = this.getAlternatives().get(0);
            this.loopBodyInputs = this.originalLoopHead.getLoopBodyInputs().stream()
                    .map(originalAlternative.getSlotMapping()::resolveUpstream).filter(Objects::nonNull)
                    .collect(Collectors.toList());
            this.initializationInputs = this.originalLoopHead.getLoopInitializationInputs().stream()
                    .map(originalAlternative.getSlotMapping()::resolveUpstream).filter(Objects::nonNull)
                    .collect(Collectors.toList());
            this.loopBodyOutputs = this.originalLoopHead.getLoopBodyOutputs().stream()
                    .map(originalAlternative.getSlotMapping()::resolveDownstream)
                    .map(RheemCollections::getSingleOrNull)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            this.finalLoopOutputs = this.originalLoopHead.getFinalLoopOutputs().stream()
                    .map(originalAlternative.getSlotMapping()::resolveDownstream)
                    .map(RheemCollections::getSingleOrNull)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        }
        return alternative;
    }

    @Override
    public Collection<OutputSlot<?>> getLoopBodyOutputs() {
        return this.loopBodyOutputs;
    }

    @Override
    public Collection<OutputSlot<?>> getFinalLoopOutputs() {
        return this.finalLoopOutputs;
    }

    @Override
    public Collection<InputSlot<?>> getLoopBodyInputs() {
        return this.loopBodyInputs;
    }

    @Override
    public Collection<InputSlot<?>> getLoopInitializationInputs() {
        return this.initializationInputs;
    }

    @Override
    public Collection<InputSlot<?>> getConditionInputSlots() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<OutputSlot<?>> getConditionOutputSlots() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getNumExpectedIterations() {
        return this.originalLoopHead.getNumExpectedIterations();
    }

    @Override
    public CardinalityPusher getCardinalityPusher(Configuration configuration) {
        return new LoopHeadAlternativeCardinalityPusher(
                this,
                this.getLoopBodyInputs(),
                this.getLoopBodyOutputs(),
                (alternative, conf) -> alternative.getContainedOperator().getCardinalityPusher(conf),
                configuration
        );
    }

    @Override
    public CardinalityPusher getInitializationPusher(Configuration configuration) {
        return new LoopHeadAlternativeCardinalityPusher(
                this,
                this.getLoopInitializationInputs(),
                this.getLoopBodyOutputs(),
                (alternative, conf) -> ((LoopHeadOperator) alternative.getContainedOperator()).getInitializationPusher(conf),
                configuration
        );
    }

    @Override
    public CardinalityPusher getFinalizationPusher(Configuration configuration) {
        return new LoopHeadAlternativeCardinalityPusher(
                this,
                this.getLoopBodyInputs(),
                this.getFinalLoopOutputs(),
                (alternative, conf) -> ((LoopHeadOperator) alternative.getContainedOperator()).getFinalizationPusher(conf),
                configuration
        );
    }
}

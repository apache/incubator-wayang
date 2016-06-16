package org.qcri.rheem.core.plan.rheemplan;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.cardinality.AggregatingCardinalityPusher;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityPusher;
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
    protected LoopHeadAlternative(LoopHeadOperator loopHead) {
        super(loopHead);
        this.originalLoopHead = loopHead;

    }

    @Override
    public void addAlternative(Operator alternative) {
        assert alternative.isLoopHead();
        assert !this.getAlternatives().isEmpty() || alternative == this.originalLoopHead;
        super.addAlternative(alternative);
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
    public int getNumExpectedIterations() {
        return this.originalLoopHead.getNumExpectedIterations();
    }

    @Override
    public CardinalityPusher getCardinalityPusher(Configuration configuration) {
        return new AggregatingCardinalityPusher(this, this.getLoopBodyInputs(), this.getLoopBodyOutputs(),
                Operator::getCardinalityPusher, configuration);
    }

    @Override
    public CardinalityPusher getInitializationPusher(Configuration configuration) {
        return new AggregatingCardinalityPusher(this,
                this.getLoopInitializationInputs(),
                this.getLoopBodyOutputs(),
                (op, conf) -> ((LoopHeadOperator) op).getInitializationPusher(conf),
                configuration);
    }

    @Override
    public CardinalityPusher getFinalizationPusher(Configuration configuration) {
        return new AggregatingCardinalityPusher(this,
                this.getLoopBodyInputs(),
                this.getLoopBodyOutputs(),
                (op, conf) -> ((LoopHeadOperator) op).getFinalizationPusher(conf),
                configuration);
    }
}

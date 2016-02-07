package org.qcri.rheem.core.optimizer.cardinality;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.plan.InputSlot;
import org.qcri.rheem.core.plan.OperatorAlternative;
import org.qcri.rheem.core.plan.OutputSlot;
import org.qcri.rheem.core.util.Tuple;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AggregatingCardinalityPusher extends CardinalityPusher {

    private final List<Tuple<OperatorAlternative.Alternative, CardinalityPusher>> pushPaths;

    public AggregatingCardinalityPusher(final OperatorAlternative operatorAlternative,
                                        final Configuration configuration,
                                        final Map<OutputSlot<?>, CardinalityEstimate> cache) {
        super(operatorAlternative, cache);
        this.pushPaths = operatorAlternative.getAlternatives().stream()
                .map(alternative -> {
                    final CardinalityPusher pusher = alternative.getOperator().getCardinalityPusher(configuration, cache);
                    return new Tuple<>(alternative, pusher);
                })
                .collect(Collectors.toList());
    }

    @Override
    public OperatorAlternative getOperator() {
        return (OperatorAlternative) super.getOperator();
    }

    @Override
    protected CardinalityEstimate[] doPush(Configuration configuration, CardinalityEstimate... inputEstimates) {
        // Simply use the estimate with the highest correctness probability.
        return this.pushPaths.stream()
                .map(pushPath -> this.pushThroughPath(pushPath, configuration, inputEstimates))
                .reduce(this::mergeEstimates)
                .orElseThrow(IllegalStateException::new);
    }

    private CardinalityEstimate[] pushThroughPath(Tuple<OperatorAlternative.Alternative, CardinalityPusher> pushPath,
                                                  Configuration configuration,
                                                  CardinalityEstimate[] inputEstimates) {
        CardinalityEstimate[] translatedEstimates = translateInputEstimates(inputEstimates, pushPath.field0);
        final CardinalityEstimate[] outputEstimates = pushPath.field1.push(configuration, translatedEstimates);
        return translateOutputEstimates(outputEstimates, pushPath.field0);
    }

    private CardinalityEstimate[] translateInputEstimates(CardinalityEstimate[] inputEstimates,
                                                          OperatorAlternative.Alternative alternative) {
        CardinalityEstimate[] translatedEstimate = new CardinalityEstimate[alternative.getOperator().getNumInputs()];
        for (int outerInputIndex = 0; outerInputIndex < this.getOperator().getAllInputs().length; outerInputIndex++) {
            InputSlot<?> outerInputSlot = this.getOperator().getAllInputs()[outerInputIndex];
            final Collection<? extends InputSlot<?>> inputSlots = alternative.getSlotMapping().resolveDownstream(outerInputSlot);
            for (InputSlot<?> innerInput : inputSlots) {
                translatedEstimate[innerInput.getIndex()] = inputEstimates[outerInputIndex];
            }

        }
        return translatedEstimate;
    }

    private CardinalityEstimate[] translateOutputEstimates(CardinalityEstimate[] inputEstimates,
                                                           OperatorAlternative.Alternative alternative) {
        CardinalityEstimate[] translatedEstimate = new CardinalityEstimate[this.getOperator().getNumOutputs()];
        for (int innerOutputIndex = 0; innerOutputIndex < alternative.getOperator().getNumOutputs(); innerOutputIndex++) {
            OutputSlot<?> innerOutput = alternative.getOperator().getOutput(innerOutputIndex);
            final Collection<? extends OutputSlot<?>> outerOutputs = alternative.getSlotMapping().resolveDownstream(innerOutput);
            for (OutputSlot<?> outerOutput : outerOutputs) {
                translatedEstimate[outerOutput.getIndex()] = inputEstimates[innerOutputIndex];
            }
        }
        return translatedEstimate;
    }

    private CardinalityEstimate[] mergeEstimates(CardinalityEstimate[] estimates1, CardinalityEstimate[] estimates2) {
        // TODO: Check if this is a good way. There are other palpable approaches (e.g., weighted average).
        for (int i = 0; i < estimates1.length; i++) {
            CardinalityEstimate estimate1 = estimates1[i];
            CardinalityEstimate estimate2 = estimates2[i];
            if (estimate2.getCorrectnessProbability() > estimate1.getCorrectnessProbability()) {
                estimates1[i] = estimate2;
            }
        }
        return estimates1;
    }

}

package org.qcri.rheem.core.mapping;

import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.platform.Platform;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * A subplan match correlates a {@link SubplanPattern} with its actually matched .
 */
public class SubplanMatch {

    /**
     * The atcual operators that have been matched to the {@link #pattern}.
     */
    private final Map<String, OperatorMatch> operatorMatches = new HashMap<>();

    /**
     * <i>Lazily initialized.</i> {@link Platform} restrictions coming from the matched {@link Operator}s.
     */
    private Optional<Set<Platform>> targetPlatforms = null;

    /**
     * The pattern that has been matched.
     */
    private final SubplanPattern pattern;

    public SubplanMatch(SubplanPattern pattern) {
        this.pattern = pattern;
    }

    /**
     * Copy constructor.
     */
    public SubplanMatch(SubplanMatch that) {
        this.pattern = that.pattern;
        this.operatorMatches.putAll(that.operatorMatches);
    }

    public void addOperatorMatch(OperatorMatch operatorMatch) {
        String name = operatorMatch.getPattern().getName();
        if (this.operatorMatches.containsKey(name)) {
            throw new IllegalArgumentException(String.format("Cannot insert operator match named \"%s\": " +
                    "a match with that name already exists.", name));
        }
        this.operatorMatches.put(name, operatorMatch);
    }

    public Map<String, OperatorMatch> getOperatorMatches() {
        return this.operatorMatches;
    }

    public SubplanPattern getPattern() {
        return this.pattern;
    }

    public OperatorMatch getInputMatch() {
        final String name = this.pattern.getInputPattern().getName();
        return this.operatorMatches.get(name);
    }

    public OperatorMatch getOutputMatch() {
        final String name = this.pattern.getOutputPattern().getName();
        return this.operatorMatches.get(name);
    }

    public OperatorMatch getMatch(String name) {
        return this.operatorMatches.get(name);
    }

    /**
     * @return the maximum epoch among the matched operators in {@link #operatorMatches}
     */
    public int getMaximumEpoch() {
        return this.operatorMatches.values().stream()
                .map(OperatorMatch::getOperator)
                .filter(Operator::isElementary)
                .mapToInt(Operator::getEpoch)
                .max()
                .orElse(Operator.FIRST_EPOCH);
    }

    /**
     * {@link Platform} restrictions coming from the matched {@link Operator}s. Notice that the semantics of empty
     * {@link Set}s differ from those in {@link Operator#getTargetPlatforms()}.
     *
     * @return the intersection of all {@link Platform} restrictions in the matched {@link Operator}s
     */
    public Optional<Set<Platform>> getTargetPlatforms() {
        if (this.targetPlatforms == null) {
            this.targetPlatforms = this.operatorMatches.values().stream()
                    .map(OperatorMatch::getOperator)
                    .map(Operator::getTargetPlatforms)
                    .filter(platforms -> !platforms.isEmpty())
                    .reduce((platforms1, platforms2) -> {
                        Set<Platform> platforms = new HashSet<>(platforms1);
                        platforms.retainAll(platforms2);
                        return platforms;
                    });
        }
        return this.targetPlatforms;
    }
}

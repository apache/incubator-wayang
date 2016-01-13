package org.qcri.rheem.core.mapping;

import java.util.HashMap;
import java.util.Map;

/**
 * A subplan match correlates a {@link SubplanPattern} with its actually matched .
 */
public class SubplanMatch {

    /**
     * The atcual operators that have been matched to the {@link #pattern}.
     */
    private final Map<String, OperatorMatch> operatorMatches = new HashMap<>();

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
        return operatorMatches;
    }

    public SubplanPattern getPattern() {
        return pattern;
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
}

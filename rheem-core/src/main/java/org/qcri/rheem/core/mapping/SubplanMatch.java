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
}

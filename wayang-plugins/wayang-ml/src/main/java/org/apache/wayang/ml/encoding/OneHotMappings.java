package org.apache.wayang.ml.encoding;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.ml.util.Operators;
import org.apache.wayang.ml.util.Platforms;

public class OneHotMappings {
    private static final int PADDING_SIZE = 1;

    private static final HashMap<String, Integer> operatorMapping = createOperatorMapping();
    private static final HashMap<String, Integer> platformsMapping = createPlatformMapping();

    public static Optional<Platform> getOperatorPlatformFromEncoding(final long[] encoded) {
        System.out.println("getOperatorPlatformFromEncoding() encoded: " + Arrays.toString(encoded));

        final int platformsCount = platformsMapping.size();
        final int operatorsCount = operatorMapping.size();

        if (platformsCount > encoded.length) {
            return Optional.empty();
        }

        int platformIndex = -1;
        final int offset = PADDING_SIZE + operatorsCount;

        for (int i = offset; i < platformsCount + offset && platformIndex == -1; i++) {
            if (encoded[i] == 1) {
                platformIndex = i;
            }
        }

        if (platformIndex == -1) {
            return Optional.empty();
        }

        for (final Object entry : platformsMapping.keySet()) {
            if (platformsMapping.get(entry).equals(platformIndex - offset)) {
                return Platforms.getPlatforms().stream().filter(pl -> pl.getName().equals(entry))
                        .map(cl -> Platform.load(cl.getName())).findAny();
            }
        }

        return Optional.empty();
    }

    public static HashMap<String, Integer> getOperatorMapping() {
        return operatorMapping;
    }

    public static HashMap<String, Integer> getPlatformsMapping() {
        return platformsMapping;
    }

    private static HashMap<String, Integer> createOperatorMapping() {
        final HashMap<String, Integer> mappings = new HashMap<>();

        Operators.getOperators().stream()
                .filter(operator -> operator.getName().contains("org.apache.wayang.basic.operators")
                        || operator.getName().contains("org.apache.wayang.core.plan.wayangplan"))
                .distinct().sorted(Comparator.comparing(Class::getName))
                .forEachOrdered(entry -> mappings.put(entry.getName(), mappings.size()));

        return mappings;
    }

    private static HashMap<String, Integer> createPlatformMapping() {
        final HashMap<String, Integer> mappings = new HashMap<>();

        Platforms.getPlatforms().stream().sorted(Comparator.comparing(Class::getName))
                .forEachOrdered(entry -> mappings.put(entry.getName(), mappings.size()));

        System.out.println("created platforms mappings: " + mappings);
        return mappings;
    }

    private final HashSet<Operator> originalOperators = new HashSet<>();

    public OneHotMappings() {
    }

    public void addOriginalOperator(final Operator operator) {
        originalOperators.add(operator);
    }

    public HashSet<Operator> getOriginalOperators() {
        return originalOperators;
    }

    public Optional<Operator> getOperatorFromEncoding(final long[] encoded) {
        final long hashCode = encoded[0];
        System.out.println("hashcode: " + hashCode);

        final Optional<Operator> original = originalOperators.stream()
                .filter(op -> 
                    (long) new HashCodeBuilder(17, 37).append(op.toString()).append(op.getName())
                        .append(op.getAllInputs().length).append(op.getAllOutputs().length).toHashCode() == hashCode)
                .findAny();

        System.out.println("original: " + original);

        if (original.isPresent()) {
            return original;
        }

        return Optional.empty();
    }
}

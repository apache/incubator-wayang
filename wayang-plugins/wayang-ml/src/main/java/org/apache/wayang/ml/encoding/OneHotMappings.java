package org.apache.wayang.ml.encoding;

import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.OperatorBase;
import org.apache.wayang.ml.util.Operators;
import org.apache.wayang.ml.util.Platforms;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.spark.operators.SparkExecutionOperator;

import org.reflections.*;
import org.reflections.scanners.SubTypesScanner;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;
import java.util.Vector;
import java.util.stream.Collectors;
import java.util.Arrays;
import java.util.Comparator;

public class OneHotMappings {

    private static OneHotMappings INSTANCE;

    private static HashMap<String, Integer> operatorMapping = createOperatorMapping();

    private static HashMap<String, Integer> platformsMapping = createPlatformMapping();

    private static HashSet<Operator> originalOperators = new HashSet<>();

    private OneHotMappings() {}

    public static OneHotMappings getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new OneHotMappings();
        }

        return INSTANCE;
    }

    public HashMap<String, Integer> getOperatorMapping() {
        return operatorMapping;
    }

    public HashMap<String, Integer> getPlatformsMapping() {
        return platformsMapping;
    }

    private static HashMap<String, Integer> createOperatorMapping() {
        HashMap<String, Integer> mappings = new HashMap<>();
        Operators.getOperators()
          .stream()
          .filter(operator -> operator.getName().contains("org.apache.wayang.basic.operators"))
          .distinct()
          .sorted(Comparator.comparing(c -> c.getName()))
          .forEach(entry -> mappings.put(entry.getName(), mappings.size()));

        // add a null operator for encoding
        mappings.put(null, mappings.size());

        return mappings;
    }

    private static HashMap<String, Integer> createPlatformMapping() {
        HashMap<String, Integer> mappings = new HashMap<>();

        Platforms.getPlatforms()
        .stream()
        .sorted(Comparator.comparing(c -> c.getName()))
        .forEach(entry -> mappings.put(entry.getName(), mappings.size()));

        return mappings;
    }

    public static void addOriginalOperator(Operator operator) {
        originalOperators.add(operator);
    }

    public static HashSet<Operator> getOriginalOperators() {
        return originalOperators;
    }

    public static Optional<Platform> getOperatorPlatformFromEncoding(long[] encoded) {
        int platformsCount = platformsMapping.size();
        int operatorsCount = operatorMapping.size();

        if (platformsCount > encoded.length) {
            return Optional.empty();
        }

        int platformIndex = -1;
        int offset = operatorsCount + 1;
        for (int i = offset; i < platformsCount + offset && platformIndex == -1; i++) {
            if(encoded[i] == 1)  {
                platformIndex = i;
            }
        }

        if (platformIndex == -1) {
            return Optional.empty();
        }

        for (final Object entry : platformsMapping.keySet()) {
            if (platformsMapping.get(entry).equals(platformIndex - operatorsCount)) {
                return Platforms.getPlatforms()
                .stream()
                .filter(pl -> pl.getName().equals(entry))
                .map(cl -> Platform.load(cl.getName()))
                .findAny();
            }
        }


        return Optional.empty();
    }

    public static Optional<Operator> getOperatorFromEncoding(long[] encoded) {
        final long hashCode = encoded[0];

        Optional<Operator> original = originalOperators.stream()
            .filter(op -> (long) new HashCodeBuilder(17, 37).append(op.toString()).toHashCode() == hashCode)
            .findAny();

        if (original.isPresent()) {
            return original;
        }

        return Optional.empty();
    }
}

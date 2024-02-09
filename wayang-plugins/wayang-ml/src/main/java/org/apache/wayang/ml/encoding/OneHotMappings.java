package org.apache.wayang.ml.encoding;

import org.apache.wayang.core.plan.wayangplan.OperatorBase;
import org.apache.wayang.ml.util.Operators;
import org.apache.wayang.ml.util.Platforms;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.spark.operators.SparkExecutionOperator;

import org.reflections.*;
import org.reflections.scanners.SubTypesScanner;

import java.util.HashMap;
import java.util.Vector;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.Comparator;

public class OneHotMappings {

    private static HashMap<String, Integer> operatorMapping = createOperatorMapping();

    private static HashMap<String, Integer> platformsMapping = createPlatformMapping();

    private OneHotMappings() {}

    public static OneHotMappings getInstance() {
        return new OneHotMappings();
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
}

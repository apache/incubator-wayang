package org.apache.wayang.ml.encoding;

import org.apache.wayang.core.plan.wayangplan.OperatorBase;
import org.apache.wayang.ml.util.Operators;
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

    private static OneHotMappings INSTANCE;

    private static HashMap<String, Integer> operatorMapping = createOperatorMapping();

    private static HashMap<String, Integer> conversionMapping = createConversionMapping();

    private OneHotMappings() {}

    public static OneHotMappings getInstance() {
        /*
        if (INSTANCE == null) {
            INSTANCE = new OneHotMappings();
        }

        return INSTANCE;*/
        return new OneHotMappings();
    }

    public HashMap<String, Integer> getOperatorMapping() {
        return operatorMapping;
    }

    public HashMap<String, Integer> getConversionMapping() {
        return conversionMapping;
    }

    // this is deterministic - don't worry
    private static HashMap<String, Integer> createOperatorMapping() {
        HashMap<String, Integer> mappings = new HashMap<>();
        Operators.getOperators()
          .stream()
          .sorted(Comparator.comparing(c -> c.getName()))
          .forEach(entry -> mappings.put(entry.getName(), mappings.size()));

        System.out.println(mappings);
        return mappings;
    }

    private static HashMap<String, Integer> createConversionMapping() {
        HashMap<String, Integer> mappings = new HashMap<>();

        Reflections java = new Reflections("org.apache.wayang.java.operators", new SubTypesScanner());
        java.getSubTypesOf(OperatorBase.class)
          .stream()
          .sorted(Comparator.comparing(c -> c.getName()))
          .forEach(entry -> mappings.put(entry.getName(), mappings.size()));

        Reflections spark = new Reflections("org.apache.wayang.spark.operators", new SubTypesScanner());
        spark.getSubTypesOf(OperatorBase.class)
          .stream()
          .sorted(Comparator.comparing(c -> c.getName()))
          .forEach(entry -> mappings.put(entry.getName(), mappings.size()));

        return mappings;
    }
}

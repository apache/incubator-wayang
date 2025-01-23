package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.wayang.api.DataQuanta;
import org.apache.wayang.api.DataQuantaBuilder;
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.api.MapDataQuantaBuilder;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.LocalCallbackSink;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.java.Java;
import org.apache.wayang.api.serialization.SerializationUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.*;

public class WordCount {

//    private Operator buildOperator(List<Double> weights, JavaPlanBuilder planBuilder){
//        Operator op = planBuilder
//                .loadCollection(weights).withName("init weights")
//                .map(value -> value + 2.0)
//                .withName("Square elements")
//                .dataQuanta()
//                .operator();
//        return op;
//    }

    private static void updateField(JsonNode node, String fieldName, String newValue) {
        if (node.isObject()) {
            ObjectNode objectNode = (ObjectNode) node;
            JsonNode field = objectNode.get(fieldName);

            if (field != null && field.isTextual()) {
                objectNode.put(fieldName, newValue);
            }
            objectNode.fields().forEachRemaining(entry -> updateField(entry.getValue(), fieldName, newValue));

        } else if (node.isArray()) {
            node.forEach(element -> updateField(element, fieldName, newValue));
        }
    }

    public static void deserializeAndExecute(String serializedPlan) throws JsonProcessingException {
//        ObjectMapper mapper = new ObjectMapper();
//        JsonNode rootNode = mapper.readTree(serializedPlan);
//        updateField(rootNode, "inputUrl", "file:/tmp/test.txt");
        // Print the updated JSON
//        serializedPlan = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(rootNode);
        WayangContext wayangContext = new WayangContext(new Configuration())
                .withPlugin(Java.basicPlugin());
        Operator op = SerializationUtils.deserializeFromString(serializedPlan, scala.reflect.ClassTag.apply(Operator.class));
        Class classType = op.getOutput(0).getType().getDataUnitType().getTypeClass();
        Collection<?> collector = new LinkedList<>();
        LocalCallbackSink<?> sink = LocalCallbackSink.createCollectingSink(collector, classType);
        op.connectTo(0, sink, 0);

        WayangPlan plan = new WayangPlan(sink);

        wayangContext.execute("WordCount", plan);

        System.out.println(collector);
    }

    public static void main(String[] args) throws JsonProcessingException {

        /* Get a plan builder */
        WayangContext wayangContext = new WayangContext(new Configuration())
                .withPlugin(Java.basicPlugin());

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("WordCount")
                .withUdfJarOf(WordCount.class);

        /* Start building the Apache WayangPlan */
//                DataQuanta dq = planBuilder
//                .loadCollection(weights).withName("init weights")
//                .map(value -> value * value)
//                .withName("Square elements")
//                .dataQuanta();

        List<Double> weights = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0);

//        TriFunction<Operator, List<Double>, JavaPlanBuilder> func = (w, pb) -> pb
//                .loadCollection(w).withName("init weights")
//                .map(value -> value + 2.0)
//                .withName("Square elements")
//                .dataQuanta()
//                .operator();
//
//        Operator op = func.apply(weights, planBuilder);
//        deserializeAndExecute(op);

//        DataQuantaBuilder<?, Double> weightsBuilder = planBuilder
//                .loadCollection(weights).withName("init weights");
//
//        DataQuantaBuilder<?, Double> updateWeights = weightsBuilder
//                .map(value -> value + 2.0)
//                .withName("Square elements");

        Operator op = planBuilder
                .loadCollection(weights).withName("init weights")
                .map(value -> value + 2.0)
                .withName("Square elements")
                .dataQuanta()
                .operator();
//
        weights = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0, 6.0);
//
//        DataQuanta dq = updateWeights.dataQuanta();
        String serializedPlan = SerializationUtils.serializeAsString(op);
//        System.out.println(serializedPlan);
        try {
            deserializeAndExecute(serializedPlan);
        } catch (JsonProcessingException e){
            System.out.println(e);
        }


    }


}


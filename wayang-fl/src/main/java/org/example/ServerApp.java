package org.example;

import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.wayang.api.DataQuanta;
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.api.serialization.SerializationUtils;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.java.Java;

import java.util.*;

public class ServerApp {

    private static String serializePlan(){
        WayangContext wayangContext = new WayangContext(new Configuration())
                .withPlugin(Java.basicPlugin());
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("WordCount")
                .withUdfJarOf(WordCount.class);
        DataQuanta dq = planBuilder
                /* Read the text file */
                .readTextFile("path").withName("Load file")

                /* Split each line by non-word characters */
                .flatMap(line -> Arrays.asList(line.split("\\W+")))
                .withName("Split words")

                /* Filter empty tokens */
                .filter(token -> !token.isEmpty())
                .withName("Filter empty words")

                /* Attach counter to each word */
                .map(word -> new Tuple2<>(word.toLowerCase(), 1)).withName("To lower case, add counter")
                .withName("To lower case, add counter")

                // Sum up counters for every word.
                .reduceByKey(
                        Tuple2::getField0,
                        (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1())
                )
                .withName("Add counters")


                /* Build the plan. This is for now hardcoded to have  stdout localsink, which should be resolved */
                .dataQuanta();

            return SerializationUtils.serializeAsString(dq.operator());
    }

    public static void main(String[] args) {
        String serializedPlan = serializePlan();

        MonoFunction<Object, Object> doubleListElements = input -> {
            if (input instanceof List<?>) {
                List<Integer> resultList = new ArrayList<>();
                for (Object elem : (List<?>) input) {
                    if (elem instanceof Integer) {
                        resultList.add(2 * (Integer) elem);
                    }
                }
                return resultList;
            }
            return input;  // Fallback in case of an unexpected type
        };

        TriFunction<Operator, List<Double>, JavaPlanBuilder> func = (w, pb) -> pb
                .loadCollection(w).withName("init weights")
                .map(value -> value + 2.0)
                .withName("Square elements")
                .dataQuanta()
                .operator();

        BiFunction<Object, Object> mergeDicts = (accCollection, inputCollection) -> {
            if (!(accCollection instanceof Collection) || !(inputCollection instanceof Collection)) {
                throw new IllegalArgumentException("Both arguments must be of type Collection<Tuple2<String, Integer>>");
            }

            Collection<Tuple2<String, Integer>> acc = (Collection<Tuple2<String, Integer>>) accCollection;
            Collection<Tuple2<String, Integer>> input = (Collection<Tuple2<String, Integer>>) inputCollection;

            Map<String, Integer> resultMap = new HashMap<>();

            for (Tuple2<String, Integer> tuple : acc) {
                resultMap.put(tuple.field0, tuple.field1);
            }

            for (Tuple2<String, Integer> tuple : input) {
                resultMap.merge(tuple.field0, tuple.field1, Integer::sum);
            }

            Collection<Tuple2<String, Integer>> result = new ArrayList<>();
            for (Map.Entry<String, Integer> entry : resultMap.entrySet()) {
                result.add(new Tuple2<>(entry.getKey(), entry.getValue()));
            }
            return result;
        };

        BiFunction<Object, Object> mergeLists = (a, b) -> {
            if (!(a instanceof Collection) || !(b instanceof Collection)) {
                throw new IllegalArgumentException("Both arguments must be of type Collection<Double>");
            }

            Collection<Double> collectionA = (Collection<Double>) a;
            Collection<Double> collectionB = (Collection<Double>) b;

            if (collectionA.size() != collectionB.size()) {
                throw new IllegalArgumentException("Both collections must have the same size.");
            }

            List<Double> result = new ArrayList<>();
            Iterator<Double> iteratorA = collectionA.iterator();
            Iterator<Double> iteratorB = collectionB.iterator();

            while (iteratorA.hasNext() && iteratorB.hasNext()) {
                result.add(iteratorA.next() + iteratorB.next());
            }

            return result;
        };

        Collection<Tuple2<String, Integer>> initialDict = new ArrayList<>();
        Collection<Tuple2<String, Integer>> emptyDict = new ArrayList<>();
        Collection<Double> initialList = new ArrayList<>(Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0));
        Collection<Double> zeroList = new ArrayList<>(Arrays.asList(0.0, 0.0, 0.0, 0.0, 0.0));
        ActorSystem system = ActorSystem.create("server-system");
        ActorRef serverActor = system.actorOf(ServerActor.props(func, mergeLists, 5, zeroList, initialList), "serverActor");
        System.out.println("Server is running...");
    }
}

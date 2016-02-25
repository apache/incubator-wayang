package org.qcri.rheem.tests;

import org.junit.Test;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.*;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.function.ExecutionContext;
import org.qcri.rheem.core.function.FlatMapDescriptor;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitType;
import org.qcri.rheem.graphchi.GraphChiPlatform;
import org.qcri.rheem.java.JavaPlatform;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Integration tests for the integration of GraphChi with Rheem.
 */
public class GraphChiIntegrationIT {

    @Test
    public void testPageRank() {
        // Get some graph data. Use the example from Wikipedia: https://en.wikipedia.org/wiki/PageRank
        Collection<char[]> adjacencies = Arrays.asList(
                new char[]{'B', 'C'},
                new char[]{'C', 'B'},
                new char[]{'D', 'A', 'B'},
                new char[]{'E', 'B', 'D', 'F'},
                new char[]{'F', 'B', 'E'},
                new char[]{'G', 'B', 'E'},
                new char[]{'H', 'B', 'E'},
                new char[]{'I', 'B', 'E'},
                new char[]{'J', 'E'},
                new char[]{'K', 'E'}
        );
        Collection<Tuple2<Character, Float>> pageRanks = Arrays.asList(
                new Tuple2<>('A', 0.033f),
                new Tuple2<>('B', 0.384f),
                new Tuple2<>('C', 0.343f),
                new Tuple2<>('D', 0.039f),
                new Tuple2<>('E', 0.081f),
                new Tuple2<>('F', 0.039f),
                new Tuple2<>('G', 0.016f),
                new Tuple2<>('H', 0.016f),
                new Tuple2<>('I', 0.016f),
                new Tuple2<>('J', 0.016f),
                new Tuple2<>('K', 0.016f)
        );

        // Create a RheemPlan:

        // Load the adjacency list.
        final CollectionSource<char[]> adjacencySource = new CollectionSource<>(adjacencies, char[].class);
        adjacencySource.setName("adjacency source");

        // Split the adjacency list into an edge list.
        FlatMapOperator<char[], Tuple2<Character, Character>> adjacencySplitter = new FlatMapOperator<>(
                new FlatMapDescriptor<>(
                        (adjacence) -> {
                            List<Tuple2<Character, Character>> result = new ArrayList<>(adjacence.length - 1);
                            for (int i = 1; i < adjacence.length; i++) {
                                result.add(new Tuple2<>(adjacence[0], adjacence[i]));
                            }
                            return result;
                        },
                        DataUnitType.createBasic(char[].class),
                        DataUnitType.<Tuple2<Character, Character>>createBasicUnchecked(Tuple2.class))
        );
        adjacencySplitter.setName("adjacency splitter");
        adjacencySource.connectTo(0, adjacencySplitter, 0);

        // Extract the vertices from the edge list.
        FlatMapOperator<Tuple2<Character, Character>, Character> vertexSplitter = new FlatMapOperator<>(
                new FlatMapDescriptor<>(
                        (edge) -> {
                            List<Character> vertices = new ArrayList<>(2);
                            vertices.add(edge.field0);
                            vertices.add(edge.field1);
                            return vertices;
                        },
                        DataUnitType.<Tuple2<Character, Character>>createBasicUnchecked(Tuple2.class),
                        DataUnitType.createBasic(Character.class)
                )
        );
        vertexSplitter.setName("vertex splitter");
        adjacencySplitter.connectTo(0, vertexSplitter, 0);

        // Find the distinct vertices.
        DistinctOperator<Character> vertexCanonicalizer = new DistinctOperator<>(Character.class);
        vertexCanonicalizer.setName("vertex canonicalizer");
        vertexSplitter.connectTo(0, vertexCanonicalizer, 0);

        // Assign an ID to each distinct vertex.
        MapOperator<Character, Tuple2<Character, Integer>> zipWithId = new MapOperator<>(
                new TransformationDescriptor<>(
                        (vertex) -> new Tuple2<>(vertex, Character.hashCode(vertex)),
                        DataUnitType.createBasic(Character.class),
                        DataUnitType.<Tuple2<Character, Integer>>createBasicUnchecked(Tuple2.class)
                )
        );
        zipWithId.setName("zip with ID");
        vertexCanonicalizer.connectTo(0, zipWithId, 0);

        // Base the edge list on vertex IDs.
        MapOperator<Tuple2<Character, Character>, Tuple2<Integer, Integer>> translate = new MapOperator<>(
                new TransformationDescriptor<>(
                        new FunctionDescriptor.ExtendedSerializableFunction<Tuple2<Character, Character>, Tuple2<Integer, Integer>>() {

                            private Map<Character, Integer> dictionary;

                            @Override
                            public void open(ExecutionContext ctx) {
                                this.dictionary = ctx.<Tuple2<Character, Integer>>getBroadcast("vertex IDs").stream().collect(
                                        Collectors.toMap(Tuple2::getField0, Tuple2::getField1)
                                );
                            }

                            @Override
                            public Tuple2<Integer, Integer> apply(Tuple2<Character, Character> in) {
                                return new Tuple2<>(this.dictionary.get(in.field0), this.dictionary.get(in.field1));
                            }
                        },
                        DataUnitType.<Tuple2<Character, Character>>createBasicUnchecked(Tuple2.class),
                        DataUnitType.<Tuple2<Integer, Integer>>createBasicUnchecked(Tuple2.class)
                )
        );
        translate.setName("translate");
        adjacencySplitter.connectTo(0, translate, 0);
        zipWithId.broadcastTo(0, translate, "vertex IDs");

        // Run the PageRank algorithm.
        PageRankOperator pageRank = new PageRankOperator(20);
        pageRank.setName("PageRank");
        translate.connectTo(0, pageRank, 0);

        // Back-translate the page ranks.
        MapOperator<Tuple2<Integer, Float>, Tuple2<Character, Float>> backtranslate = new MapOperator<>(
                new TransformationDescriptor<>(
                        new FunctionDescriptor.ExtendedSerializableFunction<Tuple2<Integer, Float>, Tuple2<Character, Float>>() {

                            private Map<Integer, Character> dictionary;

                            @Override
                            public void open(ExecutionContext ctx) {
                                this.dictionary = ctx.<Tuple2<Character, Integer>>getBroadcast("vertex IDs").stream()
                                        .map(Tuple2::swap)
                                        .collect(
                                        Collectors.toMap(Tuple2::getField0, Tuple2::getField1)
                                );
                            }

                            @Override
                            public Tuple2<Character, Float> apply(Tuple2<Integer, Float> in) {
                                return new Tuple2<>(this.dictionary.get(in.field0), in.field1);
                            }
                        },
                        DataUnitType.<Tuple2<Integer, Float>>createBasicUnchecked(Tuple2.class),
                        DataUnitType.<Tuple2<Character, Float>>createBasicUnchecked(Tuple2.class)
                )
        );
        backtranslate.setName("bracktranslate");
        pageRank.connectTo(0, backtranslate, 0);
        zipWithId.broadcastTo(0, backtranslate, "vertex IDs");

        LocalCallbackSink callbackSink = LocalCallbackSink.createStdoutSink(
                DataSetType.<Tuple2<Character, Float>>createDefaultUnchecked(Tuple2.class));
        callbackSink.setName("sink");
        backtranslate.connectTo(0, callbackSink, 0);

        RheemPlan rheemPlan = new RheemPlan(callbackSink);

        RheemContext rc = new RheemContext();
        rc.register(JavaPlatform.getInstance());
        rc.register(GraphChiPlatform.getInstance());

        rc.execute(rheemPlan);

    }

}

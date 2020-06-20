package org.qcri.rheem.tests;

import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.CartesianOperator;
import org.qcri.rheem.basic.operators.CoGroupOperator;
import org.qcri.rheem.basic.operators.CollectionSource;
import org.qcri.rheem.basic.operators.CountOperator;
import org.qcri.rheem.basic.operators.DistinctOperator;
import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.basic.operators.FlatMapOperator;
import org.qcri.rheem.basic.operators.JoinOperator;
import org.qcri.rheem.basic.operators.LocalCallbackSink;
import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.basic.operators.MapPartitionsOperator;
import org.qcri.rheem.basic.operators.ReduceByOperator;
import org.qcri.rheem.basic.operators.SortOperator;
import org.qcri.rheem.basic.operators.TextFileSink;
import org.qcri.rheem.basic.operators.TextFileSource;
import org.qcri.rheem.basic.operators.UnionAllOperator;
import org.qcri.rheem.basic.operators.ZipWithIdOperator;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.core.util.RheemArrays;

import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Provides plans that can be used for integration testing..
 */
public class RheemPlansOperators extends RheemPlans{

    public static RheemPlan cartesian(URI inputFileUri1, URI inputFileUri2, List<Tuple2<String, String>> collector){
        TextFileSource fileSource1 = new TextFileSource(inputFileUri1.toString());
        TextFileSource fileSource2 = new TextFileSource(inputFileUri2.toString());

        CartesianOperator<String, String> cartesianOperator = new CartesianOperator<String, String>(String.class, String.class);

        LocalCallbackSink<Tuple2<String, String>> sink = LocalCallbackSink.createCollectingSink(collector, ReflectionUtils.specify(Tuple2.class));

        fileSource1.connectTo(0, cartesianOperator, 0);
        fileSource2.connectTo(0, cartesianOperator, 1);
        cartesianOperator.connectTo(0, sink, 0);

        return new RheemPlan(sink);
    }

    public static RheemPlan coGroup(URI inputFileUri1, URI inputFileUri2, List<Tuple2<?, ?>> collector){
        TextFileSource fileSource1 = new TextFileSource(inputFileUri1.toString());
        TextFileSource fileSource2 = new TextFileSource(inputFileUri2.toString());

        FunctionDescriptor.SerializableFunction<String, Tuple2<String, Integer>> mapFunction =
                line -> {
                    String[] split = line.split(" ");
                    return new Tuple2<>(split[0], Integer.parseInt(split[1]));
                };

        MapOperator<String, Tuple2<String, Integer>> mapOperator1 = new MapOperator<String, Tuple2<String, Integer>>(
                mapFunction,
                String.class,
                ReflectionUtils.specify(Tuple2.class)
        );

        MapOperator<String, Tuple2<String, Integer>> mapOperator2 = new MapOperator<String, Tuple2<String, Integer>>(
                mapFunction,
                String.class,
                ReflectionUtils.specify(Tuple2.class)
        );


        FunctionDescriptor.SerializableFunction<Tuple2<String, Integer>, String> keyExtractor =
                element -> {
                    return element.field0;
                };

        CoGroupOperator<Tuple2<String, Integer>, Tuple2<String, Integer>, String> coGroupOperator =
                new CoGroupOperator<>(
                        keyExtractor,
                        keyExtractor,
                        ReflectionUtils.specify(Tuple2.class),
                        ReflectionUtils.specify(Tuple2.class),
                        String.class
                );

        LocalCallbackSink<Tuple2<?, ?>> sink =
                LocalCallbackSink.createCollectingSink(collector, ReflectionUtils.specify(Tuple2.class));

        fileSource1.connectTo(0, mapOperator1, 0);
        fileSource2.connectTo(0, mapOperator2, 0);
        mapOperator1.connectTo(0, coGroupOperator, 0);
        mapOperator2.connectTo(0, coGroupOperator, 1);
        coGroupOperator.connectTo(0, sink, 0);

        return new RheemPlan(sink);
    }

    public static RheemPlan collectionSourceOperator(Collection<String> source, Collection<String> collector){
        CollectionSource<String> colSource = new CollectionSource<String>(source, String.class);

        LocalCallbackSink<String> sink = LocalCallbackSink.createCollectingSink(collector, String.class);

        colSource.connectTo(0, sink, 0);

        return new RheemPlan(sink);
    }

    public static RheemPlan count(Collection<String> source, Collection<Long> collector){
        CollectionSource<String> colSource = new CollectionSource<String>(source, String.class);

        CountOperator<String> countOperator = new CountOperator<String>(String.class);

        LocalCallbackSink<Long> sink = LocalCallbackSink.createCollectingSink(collector, Long.class);

        colSource.connectTo(0, countOperator, 0);
        countOperator.connectTo(0, sink, 0);

        return new RheemPlan(sink);
    }

    public static RheemPlan distinct(URI inputFileUri1, Collection<String> collector){
        TextFileSource source = new TextFileSource(inputFileUri1.toString());

        MapOperator<String, String> mapOperator = new MapOperator<String, String>(
                line -> {
                    return line.toLowerCase();
                },
                String.class,
                String.class
        );

        DistinctOperator<String> distinctOperator = new DistinctOperator<String>(String.class);

        LocalCallbackSink<String> sink = LocalCallbackSink.createCollectingSink(collector, String.class);

        source.connectTo(0, mapOperator, 0);
        mapOperator.connectTo(0, distinctOperator, 0);
        distinctOperator.connectTo(0, sink, 0);

        return new RheemPlan(sink);
    }

    public static RheemPlan filter(URI inputFileUri1, Collection<String> collector){
        TextFileSource source = new TextFileSource(inputFileUri1.toString());

        FilterOperator<String> filterOperator = new FilterOperator<String>(
                line -> {
                    return line.contains("line");
                },
                String.class
        );

        LocalCallbackSink<String> sink = LocalCallbackSink.createCollectingSink(collector, String.class);

        source.connectTo(0, filterOperator, 0);
        filterOperator.connectTo(0, sink, 0);

        return new RheemPlan(sink);
    }

    public static RheemPlan flatMap(URI inputFileUri1, Collection<String> collector){
        TextFileSource source = new TextFileSource(inputFileUri1.toString());

        FlatMapOperator<String, String> flatMapOperator = new FlatMapOperator<String, String>(
                line -> {
                    return Arrays.asList(line.split(" "));
                },
                String.class,
                String.class
        );

        LocalCallbackSink<String> sink = LocalCallbackSink.createCollectingSink(collector, String.class);

        source.connectTo(0, flatMapOperator, 0);
        flatMapOperator.connectTo(0, sink, 0);

        return new RheemPlan(sink);
    }

    public static RheemPlan join(URI inputFileUri1, URI inputFileUri2, Collection<Tuple2<?, ?>> collector){
        TextFileSource source1 = new TextFileSource(inputFileUri1.toString());
        TextFileSource source2 = new TextFileSource(inputFileUri2.toString());


        FunctionDescriptor.SerializableFunction<String, Tuple2<String, String>> mapFunction =
                line -> {
                    String[] split = line.split(" ");
                    return new Tuple2<>(split[0], split[1]);
                };

        MapOperator<String, Tuple2<String, String>> mapOperator1 = new MapOperator<String, Tuple2<String, String>>(
                mapFunction,
                String.class,
                ReflectionUtils.specify(Tuple2.class)
        );

        MapOperator<String, Tuple2<String, String>> mapOperator2 = new MapOperator<String, Tuple2<String, String>>(
                mapFunction,
                String.class,
                ReflectionUtils.specify(Tuple2.class)
        );

        FunctionDescriptor.SerializableFunction<Tuple2<String, String>, String> keyFunction = tuple -> tuple.field0;

        JoinOperator<Tuple2<String, String>, Tuple2<String, String>, String> joinOperator = new JoinOperator<Tuple2<String, String>, Tuple2<String, String>, String>(
                keyFunction,
                keyFunction,
                ReflectionUtils.specify(Tuple2.class),
                ReflectionUtils.specify(Tuple2.class),
                String.class
        );

        LocalCallbackSink<Tuple2<?, ?>> sink = LocalCallbackSink.createCollectingSink(collector, ReflectionUtils.specify(Tuple2.class));

        source1.connectTo(0, mapOperator1, 0);
        source2.connectTo(0, mapOperator2, 0);
        mapOperator1.connectTo(0, joinOperator, 0);
        mapOperator2.connectTo(0, joinOperator, 1);
        joinOperator.connectTo(0, sink, 0);

        return new RheemPlan(sink);
    }


    public static RheemPlan reduceBy(URI inputFileUri1, Collection<Tuple2<?, ?>> collector){
        TextFileSource source = new TextFileSource(inputFileUri1.toString());

        FunctionDescriptor.SerializableFunction<String, Tuple2<String, String>> mapFunction =
            line -> {
                String[] split = line.split(" ");
                return new Tuple2<>(split[0], split[1]);
            };

        MapOperator<String, Tuple2<String, String>> mapOperator = new MapOperator<String, Tuple2<String, String>>(
                mapFunction,
                String.class,
                ReflectionUtils.specify(Tuple2.class)
        );


        FunctionDescriptor.SerializableFunction<Tuple2<String, String>, String> keyFunction = tuple -> tuple.field0;

        ReduceByOperator<Tuple2<String, String>, String> reduceByOperator = new ReduceByOperator<Tuple2<String, String>, String>(
                keyFunction,
                (tuple, tuple2) -> {
                    return new Tuple2<>(tuple.field0, tuple.field1+" - "+tuple2.field1);
                },
                String.class,
                ReflectionUtils.specify(Tuple2.class)
        );

        LocalCallbackSink<Tuple2<?, ?>> sink = LocalCallbackSink.createCollectingSink(collector, ReflectionUtils.specify(Tuple2.class));

        source.connectTo(0, mapOperator, 0);
        mapOperator.connectTo(0, reduceByOperator, 0);
        reduceByOperator.connectTo(0, sink, 0);

        return new RheemPlan(sink);
    }

    public static RheemPlan sort(URI inputFileUri1, Collection<String> collector){
        TextFileSource source = new TextFileSource(inputFileUri1.toString());

        FunctionDescriptor.SerializableFunction<String, Iterable<String>> flatMapFunction =
                line -> {
                    return  Arrays.asList(line.split(" "));
                };

        FlatMapOperator<String, String> flatMapOperator = new FlatMapOperator<String, String>(
                flatMapFunction,
                String.class,
                String.class
        );

        SortOperator<String, String> sortOperator = new SortOperator<String, String>(
                word -> word,
                String.class,
                String.class
        );


        LocalCallbackSink<String> sink = LocalCallbackSink.createCollectingSink(collector, String.class);

        source.connectTo(0, flatMapOperator, 0);
        flatMapOperator.connectTo(0, sortOperator, 0);
        sortOperator.connectTo(0, sink, 0);

        return new RheemPlan(sink);
    }

    public static RheemPlan textFileSink(URI inputFileUri1, URI outputFileUri1){
        TextFileSource source = new TextFileSource(inputFileUri1.toString());

        TextFileSink<String> sink = new TextFileSink<String>(outputFileUri1.toString(), String.class);

        source.connectTo(0, sink, 0);

        return new RheemPlan(sink);
    }

    public static RheemPlan union(URI inputFileUri1, URI inputFileUri2, Collection<String> collector){
        TextFileSource source1 = new TextFileSource(inputFileUri1.toString());
        TextFileSource source2 = new TextFileSource(inputFileUri2.toString());

        UnionAllOperator<String> unionAllOperator = new UnionAllOperator<String>(String.class);

        LocalCallbackSink<String> sink = LocalCallbackSink.createCollectingSink(collector, String.class);

        source1.connectTo(0, unionAllOperator, 0);
        source2.connectTo(0, unionAllOperator, 1);
        unionAllOperator.connectTo(0, sink, 0);

        return new RheemPlan(sink);
    }

    public static RheemPlan zipWithId(URI inputFileUri1, Collection<Tuple2<Long, String>> collector){
        TextFileSource source1 = new TextFileSource(inputFileUri1.toString());

        ZipWithIdOperator<String> zipWithIdOperator = new ZipWithIdOperator<String>(String.class);

        LocalCallbackSink<Tuple2<Long, String>> sink = LocalCallbackSink.createCollectingSink(collector, ReflectionUtils.specify(Tuple2.class));

        source1.connectTo(0, zipWithIdOperator, 0);
        zipWithIdOperator.connectTo(0, sink, 0);

        return new RheemPlan(sink);
    }

    public static RheemPlan mapPartitions(Collection<Tuple2<String, Integer>> collector, int... inputValues) {
        CollectionSource<Integer> source = new CollectionSource<Integer>(RheemArrays.asList(inputValues), Integer.class);

        MapPartitionsOperator<Integer, Tuple2<String, Integer>> mapPartition = new MapPartitionsOperator<Integer, Tuple2<String, Integer>>(
                partition -> {
                    int numEvens = 0, numOdds = 0;
                    for (Integer value : partition) {
                        if ((value & 1) == 0) numEvens++;
                        else numOdds++;
                    }
                    return Arrays.asList(
                            new Tuple2<>("odd", numOdds),
                            new Tuple2<>("even", numEvens)
                    );
                },
                Integer.class,
                ReflectionUtils.specify(Tuple2.class)
        );

        FunctionDescriptor.SerializableFunction<Tuple2<String, Integer>, String> keyFunction = tuple -> tuple.field0;

        ReduceByOperator<Tuple2<String, Integer>, String> reduceByOperator = new ReduceByOperator<Tuple2<String, Integer>, String>(
                keyFunction,
                (t1, t2) -> {
                    return new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1());
                },
                String.class,
                ReflectionUtils.specify(Tuple2.class)
        );

        LocalCallbackSink<Tuple2<String, Integer>> sink = LocalCallbackSink.createCollectingSink(collector,  ReflectionUtils.specify(Tuple2.class));


        source.connectTo(0, mapPartition, 0);
        mapPartition.connectTo(0, reduceByOperator, 0);
        reduceByOperator.connectTo(0, sink, 0);

        return new RheemPlan(sink);

    }
}

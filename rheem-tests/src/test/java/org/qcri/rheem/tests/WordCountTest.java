package org.qcri.rheem.tests;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.*;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.FlatMapDescriptor;
import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.types.BasicDataUnitType;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitGroupType;
import org.qcri.rheem.core.types.DataUnitType;
import org.qcri.rheem.java.plugin.JavaPlatform;
import org.qcri.rheem.spark.platform.SparkPlatform;
import org.qcri.rheem.tests.platform.MyMadeUpPlatform;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Word count test.
 */
public class WordCountTest {

    @Test
    public void testReadAndWrite() throws URISyntaxException, IOException {

        // Instantiate Rheem and activate the backend.
        RheemContext rheemContext = new RheemContext();
        rheemContext.register(JavaPlatform.getInstance());
//        rheemContext.register(SparkPlatform.getInstance());

        TextFileSource textFileSource = new TextFileSource(RheemPlans.FILE_SOME_LINES_TXT.toString());

        /*
            Create Rheem operators.
         */


        // for each line (input) output an iterator of the words
        FlatMapOperator flatMapOperator = new FlatMapOperator<>(
                DataSetType.createDefault(String.class),
                DataSetType.createDefault(String.class),
                new FlatMapDescriptor<>(line -> Arrays.asList(line.split(" ")).iterator(),
                        DataUnitType.createBasic(String.class),
                        DataUnitType.createBasic(String.class)
                ));


        // for each word transform it to lowercase and output a key-value pair (word, 1)
        MapOperator mapOperator = new MapOperator<>(
                DataSetType.createDefault(String.class),
                DataSetType.createDefault(Tuple2.class),
                new TransformationDescriptor<>(word -> new Tuple2(word.toLowerCase(), 1),
                DataUnitType.createBasic(String.class),
                DataUnitType.createBasic(Tuple2.class)
                ));


        // groupby the key (word) and add up the values (frequency)
        ReduceByOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> reduceByOperator = new ReduceByOperator(
                DataSetType.createDefault(Tuple2.class),
                new TransformationDescriptor<Tuple2<String, Integer>, String>(pair -> pair.field0, DataUnitType.createBasic(Tuple2.class), DataUnitType.createBasic(String.class)),
                new ReduceDescriptor<Tuple2<String, Integer>>(DataUnitType.createGroupedUnchecked(Tuple2.class), DataUnitType.createBasicUnchecked(Tuple2.class),
                        ((a, b) -> {
                            a.field1 += b.field1;
                            return a;
                        }))
        );


        // write results to a sink
        List<Tuple2> results = new ArrayList<>();
        LocalCallbackSink<Tuple2> sink = LocalCallbackSink.createCollectingSink(results, DataSetType.createDefault(Tuple2.class));

        // Build Rheem plan by connecting operators
        textFileSource.connectTo(0, flatMapOperator, 0);
        flatMapOperator.connectTo(0, mapOperator, 0);
        mapOperator.connectTo(0, reduceByOperator, 0);
        reduceByOperator.connectTo(0, sink, 0);
        RheemPlan rheemPlan = new RheemPlan();
        rheemPlan.addSink(sink);

        // Have Rheem execute the plan.
        rheemContext.execute(rheemPlan);

        System.out.println("results: " + results);

        // Verify the plan result.
        Hashtable<String, Integer> map = new Hashtable<>();
        List<Tuple2> correctResults = new ArrayList<>();
        final List<String> lines = Files.lines(Paths.get(RheemPlans.FILE_SOME_LINES_TXT)).collect(Collectors.toList());
        lines.forEach((line) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                Integer freq;
                if ((freq = map.get(word.toLowerCase())) != null) { //already there
                    freq++;
                    map.put(word.toLowerCase(), freq);
                }
                else
                    map.put(word.toLowerCase(), 1);
            }
        });
        map.keySet().forEach((key) -> {
            correctResults.add(new Tuple2(key, map.get(key)));
        });
        System.out.println(correctResults);
        Assert.assertTrue(results.size()== correctResults.size() && results.containsAll(correctResults) && correctResults.containsAll(results));
    }


}

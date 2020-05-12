package org.qcri.rheem.flink.compiler;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.util.Collector;
import org.qcri.rheem.basic.data.Tuple2;

/**
 * Wrapper of {@Link CoGroupFunction} of Flink for use in Rheem
 */
public class FlinkCoGroupFunction<InputType0, InputType1, OutputType> implements CoGroupFunction<InputType0, InputType1, OutputType> {


    @Override
    public void coGroup(Iterable<InputType0> iterable, Iterable<InputType1> iterable1, Collector<OutputType> collector) throws Exception {
        collector.collect((OutputType) new Tuple2<Iterable<InputType0>, Iterable<InputType1>>(iterable, iterable1));
    }
}

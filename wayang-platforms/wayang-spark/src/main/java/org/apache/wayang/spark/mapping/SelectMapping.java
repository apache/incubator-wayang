package org.apache.wayang.spark.mapping;

import org.apache.wayang.basic.operators.ParquetSink;
import org.apache.wayang.core.mapping.*;
import org.apache.wayang.spark.operators.SelectOperator;
import org.apache.wayang.spark.operators.SparkSelectOperator;
import org.apache.wayang.spark.platform.SparkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * inspired by similar classes (e.g. FilterMapping)
 */
public class SelectMapping implements Mapping {
    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                SparkPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        OperatorPattern<ParquetSink> operatorPattern = new OperatorPattern<>(
                //SelectOperator lacks constructor
                "select", new SelectOperator(), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<SelectOperator>(
                (matchedOperator, epoch) -> new SparkSelectOperator(matchedOperator).at(epoch)
        );
    }
}

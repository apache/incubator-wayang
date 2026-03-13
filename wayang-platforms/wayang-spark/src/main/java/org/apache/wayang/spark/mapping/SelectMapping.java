package org.apache.wayang.spark.mapping;

import org.apache.wayang.basic.data.Row;
import org.apache.wayang.basic.operators.SelectOperator;
import org.apache.wayang.core.mapping.*;
import org.apache.wayang.core.types.DataSetType;
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
        OperatorPattern<SelectOperator> operatorPattern = new OperatorPattern<>(
                "select", new SelectOperator(null, DataSetType.createDefault(Row.class)), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<SelectOperator>(
                (matchedOperator, epoch) -> new SparkSelectOperator(matchedOperator).at(epoch)
        );
    }
}

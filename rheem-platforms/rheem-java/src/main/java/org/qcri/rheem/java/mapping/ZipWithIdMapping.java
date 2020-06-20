package org.qcri.rheem.java.mapping;

import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.ZipWithIdOperator;
import org.qcri.rheem.core.function.ExecutionContext;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.mapping.OperatorPattern;
import org.qcri.rheem.core.mapping.PlanTransformation;
import org.qcri.rheem.core.mapping.ReplacementSubplanFactory;
import org.qcri.rheem.core.mapping.SubplanPattern;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.operators.JavaMapOperator;
import org.qcri.rheem.java.platform.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link ZipWithIdMapping} to a subplan.
 */
@SuppressWarnings("unchecked")
public class ZipWithIdMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                JavaPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "zipwithid", new ZipWithIdOperator<>(DataSetType.none()), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<ZipWithIdOperator<Object>>(
                (matchedOperator, epoch) -> {
                    final DataSetType<Object> inputType = matchedOperator.getInputType();
                    final DataSetType<Tuple2<Long, Object>> outputType = matchedOperator.getOutputType();
                    return new JavaMapOperator<>(
                            inputType,
                            outputType,
                            new TransformationDescriptor<>(
                                    new FunctionDescriptor.ExtendedSerializableFunction<Object, Tuple2<Long, Object>>() {

                                        private long nextId;

                                        @Override
                                        public void open(ExecutionContext ctx) {
                                            this.nextId = 0L;
                                        }

                                        @Override
                                        public Tuple2<Long, Object> apply(Object o) {
                                            return new Tuple2<>(this.nextId++, o);
                                        }
                                    },
                                    inputType.getDataUnitType().toBasicDataUnitType(),
                                    outputType.getDataUnitType().toBasicDataUnitType()
                            )
                    ).at(epoch);
                }
        );
    }
}

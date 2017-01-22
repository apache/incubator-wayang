package org.qcri.rheem.core.plan.rheemplan;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.core.util.RheemCollections;

import java.util.Collection;

/**
 * Test suite for the {@link Operator} class.
 */
public class OperatorTest {

    public class Operator1 extends OperatorBase {

        @EstimationContextProperty
        protected final double op1Property;

        protected final double someOtherProperty = 0d;

        public Operator1(double op1Property) {
            super(0, 0, false);
            this.op1Property = op1Property;
        }

        public double getOp1Property() {
            return this.op1Property;
        }

    }

    public class Operator2 extends Operator1 {

        @EstimationContextProperty
        private final double op2Property;

        public Operator2(double op1Property, double op2Property) {
            super(op1Property);
            this.op2Property = op2Property;
        }

        public double getOp2Property() {
            return this.op2Property;
        }
    }

    @Test
    public void testPropertyDetection() {
        Operator op = new Operator2(0, 1);
        final Collection<String> estimationContextProperties = op.getEstimationContextProperties();
        Assert.assertEquals(
                RheemCollections.asSet("op1Property", "op2Property"),
                RheemCollections.asSet(estimationContextProperties)
        );
    }

    @Test
    public void testPropertyCollection() {
        Operator op = new Operator2(0, 1);
        Assert.assertEquals(
                Double.valueOf(0d),
                ReflectionUtils.getProperty(op, "op1Property")
        );
        Assert.assertEquals(
                Double.valueOf(1d),
                ReflectionUtils.getProperty(op, "op2Property")
        );
    }


}

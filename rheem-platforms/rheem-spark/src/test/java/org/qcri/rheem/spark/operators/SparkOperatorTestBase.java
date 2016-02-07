package org.qcri.rheem.spark.operators;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import org.qcri.rheem.spark.plugin.Activator;

/**
 * Created by yidris on 2/6/16.
 */
public class SparkOperatorTestBase {

    public  JavaSparkContext getSC() {return Activator.sc;};
    public SparkOperatorTestBase() {
    }

}

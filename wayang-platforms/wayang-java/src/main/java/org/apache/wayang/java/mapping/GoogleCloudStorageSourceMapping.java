package org.apache.wayang.java.mapping;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.apache.wayang.basic.operators.GoogleCloudStorageSource;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.mapping.OperatorPattern;
import org.apache.wayang.core.mapping.PlanTransformation;
import org.apache.wayang.core.mapping.ReplacementSubplanFactory;
import org.apache.wayang.core.mapping.SubplanPattern;
import org.apache.wayang.java.operators.JavaGoogleCloudStorageSource;
import org.apache.wayang.java.platform.JavaPlatform;

/**
 * Mapping from {@link GoogleCloudStorageSource} to {@link JavaGoogleClodStorageSource}.
 */
public class GoogleCloudStorageSourceMapping implements Mapping {
    
        @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                JavaPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern(){
        final OperatorPattern operatorPattern = new OperatorPattern(
                "source", 
                new org.apache.wayang.basic.operators.GoogleCloudStorageSource((String) null, (String) null, (String) null),
                false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<GoogleCloudStorageSource>(
                (matchedOperator, epoch) -> new JavaGoogleCloudStorageSource(matchedOperator).at(epoch)
        );
    }

}

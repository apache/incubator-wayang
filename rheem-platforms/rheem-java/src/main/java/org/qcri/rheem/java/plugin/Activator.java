package org.qcri.rheem.java.plugin;

import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.java.mapping.*;
import org.qcri.rheem.java.platform.JavaExecutor;

/**
 * Activator for the Java platform binding for Rheem.
 */
public class Activator {

    public static final Platform PLATFORM = new Platform("java", JavaExecutor.FACTORY);

    public static void registerTo(RheemContext rheemContext) {
        rheemContext.register(new TextFileSourceToJavaTextFileSourceMapping());
        rheemContext.register(new StdoutSinkToJavaStdoutSinkMapping());
        rheemContext.register(new MapOperatorToJavaMapOperatorMapping());
        rheemContext.register(new ReduceByOperatorToJavaReduceByOperatorMapping());
        rheemContext.register(new JavaCollectionSourceMapping());
        rheemContext.register(new JavaLocalCallbackSinkMapping());
    }

}

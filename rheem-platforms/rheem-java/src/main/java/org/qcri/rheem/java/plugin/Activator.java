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

    public static void activate(RheemContext rheemContext) {
        rheemContext.register(new TextFileSourceToJavaTextFileSourceMapping());
        rheemContext.register(new StdoutSinkToJavaStdoutSinkMapping());
        rheemContext.register(new MapOperatorToJavaMapOperatorMapping());
        rheemContext.register(new ReduceByOperatorToJavaReduceByOperatorMapping());
        rheemContext.register(new JavaCollectionSourceMapping());
        rheemContext.register(new JavaLocalCallbackSinkMapping());
        rheemContext.register(new JavaGlobalReduceOperatorMapping());
        rheemContext.register(new JavaCollocateByOperatorMapping());
        rheemContext.register(new FlatMapToJavaFlatMapMapping());
        rheemContext.register(new CountToJavaCountMapping());
        rheemContext.register(new DistinctToJavaDistinctMapping());
        rheemContext.register(new SortToJavaSortMapping());
        rheemContext.register(new FilterToJavaFilterMapping());
    }

}

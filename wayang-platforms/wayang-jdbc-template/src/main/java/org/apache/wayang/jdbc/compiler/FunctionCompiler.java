package org.apache.incubator.wayang.jdbc.compiler;

import org.apache.incubator.wayang.core.function.FunctionDescriptor;
import org.apache.incubator.wayang.core.function.PredicateDescriptor;

/**
 * Compiles {@link FunctionDescriptor}s to SQL clauses.
 */
public class FunctionCompiler {

    /**
     * Compile a predicate to a SQL {@code WHERE} clause.
     *
     * @param descriptor describes the predicate
     * @return a compiled SQL {@code WHERE} clause
     */
    public String compile(PredicateDescriptor descriptor) {
        final String sqlImplementation = descriptor.getSqlImplementation();
        assert sqlImplementation != null;
        return sqlImplementation;
    }

}

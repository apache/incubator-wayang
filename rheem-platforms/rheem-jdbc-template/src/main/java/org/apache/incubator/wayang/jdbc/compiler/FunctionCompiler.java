package io.rheem.rheem.jdbc.compiler;

import io.rheem.rheem.core.function.FunctionDescriptor;
import io.rheem.rheem.core.function.PredicateDescriptor;

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

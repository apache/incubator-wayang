package org.qcri.rheem.jdbc.operators;

import org.qcri.rheem.basic.function.ProjectionDescriptor;
import org.qcri.rheem.basic.operators.ProjectionOperator;
import org.qcri.rheem.jdbc.compiler.FunctionCompiler;

import java.sql.Connection;

/**
 * PostgreSQL implementation for the {@link ProjectionOperator}.
 */
public abstract class JdbcProjectionOperator<InputType, OutputType> extends ProjectionOperator<InputType, OutputType>
        implements JdbcExecutionOperator {


    public JdbcProjectionOperator(Class<InputType> inputTypeClass, Class<OutputType> outputTypeClass,
                                  String... fieldNames) {
        super(inputTypeClass, outputTypeClass, fieldNames);
    }

    public JdbcProjectionOperator(Class<InputType> inputTypeClass, Class<OutputType> outputTypeClass,
                                  Integer... fieldIndexes) {
        super(inputTypeClass, outputTypeClass, fieldIndexes);
    }

    public JdbcProjectionOperator(ProjectionDescriptor<InputType, OutputType> functionDescriptor) {
        super(functionDescriptor);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JdbcProjectionOperator(ProjectionOperator<InputType, OutputType> that) {
        super(that);
    }

    @Override
    public String createSqlClause(Connection connection, FunctionCompiler compiler) {
        return String.join(", ", this.getFunctionDescriptor().getFieldNames());
    }

}

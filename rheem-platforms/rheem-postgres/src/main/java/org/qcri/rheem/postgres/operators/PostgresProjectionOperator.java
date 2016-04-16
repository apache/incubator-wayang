package org.qcri.rheem.postgres.operators;

import org.qcri.rheem.basic.function.ProjectionDescriptor;
import org.qcri.rheem.basic.operators.ProjectionOperator;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.postgres.compiler.FunctionCompiler;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yidris on 3/23/16.
 */
public class PostgresProjectionOperator<InputType, OutputType> extends ProjectionOperator
        implements PostgresExecutionOperator{


    public PostgresProjectionOperator(Class<InputType> inputTypeClass, Class<OutputType> outputTypeClass,
                              String... fieldNames) {
        super(inputTypeClass, outputTypeClass, fieldNames);
    }

    public PostgresProjectionOperator(Class<InputType> inputTypeClass, Class<OutputType> outputTypeClass,
                              Integer... fieldIndexes) {
        super(inputTypeClass, outputTypeClass, fieldIndexes);
    }

    public PostgresProjectionOperator(ProjectionDescriptor<InputType, OutputType> functionDescriptor) {
        super(functionDescriptor);
    }

    public String evaluate(Channel[] inputChannels, Channel[] outputChannels, FunctionCompiler compiler) {
        List<String> colNames = this.getFunctionDescriptor().getFieldNames();
        return String.join(",", colNames);
    }

    public String evaluateByIndexes(Channel[] inputChannels, Channel[] outputChannels, FunctionCompiler compiler,
                                    Connection conn, String selectQuery) throws SQLException {
        Statement stmt = conn.createStatement();
        ResultSet metaRs = stmt.executeQuery(selectQuery + " limit 1");
        List<String> colNames = new ArrayList<>();
        List<Integer> colIndexes = this.getFunctionDescriptor().getFieldIndexes();
        for (Integer index : colIndexes){
            //postgres column index starts at 1
            colNames.add(metaRs.getMetaData().getColumnName(index+1));
        }
        this.getFunctionDescriptor().setFieldNames(colNames);
        stmt.close();
        metaRs.close();
        return evaluate(inputChannels, outputChannels, compiler);
    }

}

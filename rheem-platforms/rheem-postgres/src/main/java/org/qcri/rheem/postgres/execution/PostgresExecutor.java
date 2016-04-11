package org.qcri.rheem.postgres.execution;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.platform.ExecutionProfile;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.util.fs.FileSystem;
import org.qcri.rheem.core.util.fs.FileSystems;
import org.qcri.rheem.postgres.PostgresPlatform;
import org.qcri.rheem.postgres.compiler.FunctionCompiler;
import org.qcri.rheem.postgres.operators.PostgresFilterOperator;
import org.qcri.rheem.postgres.operators.PostgresProjectionOperator;
import org.qcri.rheem.postgres.operators.PostgresTableSource;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.sql.*;
import java.util.Collection;
import java.util.Set;

/**
 * Created by yidris on 3/23/16.
 */
public class PostgresExecutor implements Executor {

    private PostgresPlatform platform;

    public PostgresExecutor(PostgresPlatform platform) {
        this.platform = platform;
    }

    @Override
    public ExecutionProfile execute(ExecutionStage stage) {
        ResultSet rs = null;
        FunctionCompiler functionCompiler = new FunctionCompiler();
        Collection<?> startTasks = stage.getStartTasks();
        Collection<?> termTasks = stage.getTerminalTasks();

        Validate.isTrue(startTasks.size()==1, "Invalid postgres stage: multiple sources are not currently supported");
        ExecutionTask startTask = (ExecutionTask) startTasks.toArray()[0];

        Validate.isTrue(termTasks.size()==1,
                "Invalid postgres stage: multiple terminal tasks are not currently supported");
        ExecutionTask termTask = (ExecutionTask) termTasks.toArray()[0];

        Validate.isTrue(startTask.getOperator() instanceof PostgresTableSource,
                "Invalid postgres stage: Start task has to be a PostgresTableSource");

        PostgresTableSource tableOp = (PostgresTableSource) startTask.getOperator();
        PostgresFilterOperator filterOp = null;
        PostgresProjectionOperator projectionOperator = null;

        Set<ExecutionTask> allTasks = stage.getAllTasks();
        assert allTasks.size()<=3;
        for (ExecutionTask t: allTasks) {
            if (t==startTask)
                continue;
            if (t.getOperator() instanceof PostgresFilterOperator){
                assert filterOp==null; // Allow one filter operator per stage for now.
                filterOp = (PostgresFilterOperator)t.getOperator();
            }
            else if (t.getOperator() instanceof PostgresProjectionOperator){
                assert projectionOperator==null; //Allow one projection operator per stage for now.
                projectionOperator = (PostgresProjectionOperator)t.getOperator();

            }
            else{
                throw new RheemException(String.format("Invalid postgres execution task %s", t.toString()));
            }
        }

        Connection connection = platform.getConnection();
        PreparedStatement ps = null;
        String query = "";
        String select_columns = "*";
        String where_clause = "";
        query = tableOp.evaluate(null, null, functionCompiler);

        if (projectionOperator!=null) {
            if (projectionOperator.isProjectByIndexes()){
                try {
                    select_columns = projectionOperator.evaluateByIndexes(null, null,
                            functionCompiler, connection, String.format(query, "*"));
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new RheemException(e);
                }
            }
            else
                select_columns = projectionOperator.evaluate(null, null, functionCompiler);
        }
        query = String.format(query, select_columns);

        if(filterOp!=null) {
            where_clause = filterOp.evaluate(null, null, functionCompiler);
            query = query + " where " + where_clause;
        }

        try {
            ps = connection.prepareStatement(query);
            rs = ps.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RheemException(e);
        }
        final FileChannel outputFileChannel = (FileChannel) termTask.getOutputChannel(0);
        try {
            this.saveResult(outputFileChannel, rs);
            rs.close();
            ps.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RheemException(e);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RheemException(e);
        }
        return new ExecutionProfile();

    }

    @Override
    public void dispose() {

    }

    @Override
    public Platform getPlatform() {
        return platform;
    }


    private void saveResult(FileChannel outputFileChannel, ResultSet rs) throws IOException, SQLException {
        // Output results.
        final FileSystem outFs = FileSystems.getFileSystem(outputFileChannel.getSinglePath()).get();
        try (final OutputStreamWriter writer = new OutputStreamWriter(outFs.create(outputFileChannel.getSinglePath()))) {
            while ( rs.next() ) {
                //System.out.println(rs.getInt(1) + " " + rs.getString(2));
                ResultSetMetaData rsmd = rs.getMetaData();
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    writer.write(rs.getString(i));
                    if (i < rsmd.getColumnCount()) {
                        writer.write('\t');
                    }
                }
                if (!rs.isLast()){
                    writer.write('\n');
                }
            }
        }
         catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }
}

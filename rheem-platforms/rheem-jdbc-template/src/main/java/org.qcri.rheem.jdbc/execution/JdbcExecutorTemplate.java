package org.qcri.rheem.jdbc.execution;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.basic.operators.ProjectionOperator;
import org.qcri.rheem.basic.operators.TableSource;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.platform.*;
import org.qcri.rheem.core.util.fs.FileSystem;
import org.qcri.rheem.core.util.fs.FileSystems;
import org.qcri.rheem.jdbc.JdbcPlatformTemplate;
import org.qcri.rheem.jdbc.compiler.FunctionCompiler;
import org.qcri.rheem.jdbc.operators.JdbcExecutionOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@link Executor} implementation for the {@link JdbcPlatformTemplate}.
 */
public class JdbcExecutorTemplate extends ExecutorTemplate {

    private final JdbcPlatformTemplate platform;

    private final Connection connection;

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final FunctionCompiler functionCompiler = new FunctionCompiler();

    public JdbcExecutorTemplate(JdbcPlatformTemplate platform, Job job) {
        super(job.getCrossPlatformExecutor());
        this.platform = platform;
        this.connection = this.createJdbcConnection(job.getConfiguration());
    }

    private Connection createJdbcConnection(Configuration configuration) {
        try {
            Class.forName(this.platform.getJdbcDriverClassName());
            return DriverManager.getConnection(
                    configuration.getStringProperty(this.platform.jdbcUserProperty),
                    configuration.getStringProperty(this.platform.jdbcUserProperty, null),
                    configuration.getStringProperty(this.platform.jdbcPasswordProperty, null)
            );
        } catch (Exception e) {
            throw new RheemException(String.format("Could not connect to %s.", this.platform.getName()), e);
        }
    }

    @Override
    public void execute(ExecutionStage stage, OptimizationContext optimizationContext, ExecutionState executionState) {
        // TODO: Load ChannelInstances from executionState? (as of now there is no input into PostgreSQL).
        Collection<?> startTasks = stage.getStartTasks();
        Collection<?> termTasks = stage.getTerminalTasks();

        // Verify that we can handle this instance.
        Validate.isTrue(startTasks.size() == 1, "Invalid jdbc stage: multiple sources are not currently supported");
        ExecutionTask startTask = (ExecutionTask) startTasks.toArray()[0];
        Validate.isTrue(termTasks.size() == 1,
                "Invalid JDBC stage: multiple terminal tasks are not currently supported");
        ExecutionTask termTask = (ExecutionTask) termTasks.toArray()[0];
        Validate.isTrue(startTask.getOperator() instanceof TableSource<?>,
                "Invalid JDBC stage: Start task has to be a TableSource");

        TableSource tableOp = (TableSource) startTask.getOperator();
        Collection<ExecutionTask> filterTasks = new ArrayList<>(4);
        ExecutionTask projectionTask = null;

        Set<ExecutionTask> allTasks = stage.getAllTasks();
        assert allTasks.size() <= 3;
        for (ExecutionTask t : allTasks) {
            if (t == startTask)
                continue;
            if (t.getOperator() instanceof FilterOperator<?>) {
                filterTasks.add(t);
            } else if (t.getOperator() instanceof ProjectionOperator<?, ?>) {
                assert projectionTask == null; //Allow one projection operator per stage for now.
                projectionTask = t;

            } else {
                throw new RheemException(String.format("Unsupported JDBC execution task %s", t.toString()));
            }
        }

        String tableName = this.getSqlClause(tableOp);
        Collection<String> conditions = filterTasks.stream()
                .map(ExecutionTask::getOperator)
                .map(this::getSqlClause)
                .collect(Collectors.toList());
        String projection = projectionTask == null ? "*" : this.getSqlClause(projectionTask.getOperator());

        StringBuilder sb = new StringBuilder(1000);
        sb.append("SELECT ").append(projection);
        if (!conditions.isEmpty()) {
            sb.append(" WHERE ");
            String separator = "";
            for (String condition : conditions) {
                sb.append(separator).append(condition);
                separator = " AND ";
            }
        }
        sb.append(" FROM ").append(tableName).append(';');
        String query = sb.toString();

        try (final PreparedStatement ps = this.connection.prepareStatement(query)) {
            final ResultSet rs = ps.executeQuery();
            final FileChannel.Instance outputFileChannelInstance =
                    (FileChannel.Instance) termTask.getOutputChannel(0).createInstance(this, null, 0);
//            outputFileChannelInstance.addPredecessor(tipChannelInstance);
            this.saveResult(outputFileChannelInstance, rs);
            executionState.register(outputFileChannelInstance);
        } catch (IOException | SQLException e) {
            throw new RheemException("PostgreSQL execution failed.", e);
        }

        // TODO: Set output ChannelInstance correctly.
        // TODO: Use StreamChannel instead of TSV files.
    }

    /**
     * Creates a SQL clause that corresponds to the given {@link Operator}.
     *
     * @param operator for that the SQL clause should be generated
     * @return the SQL clause
     */
    private String getSqlClause(Operator operator) {
        return ((JdbcExecutionOperator) operator).createSqlClause(this.connection, this.functionCompiler);
    }

    @Override
    public void dispose() {
        try {
            this.connection.close();
        } catch (SQLException e) {
            this.logger.error("Could not close JDBC connection to PostgreSQL correctly.", e);
        }
    }

    @Override
    public Platform getPlatform() {
        return this.platform;
    }


    private void saveResult(FileChannel.Instance outputFileChannelInstance, ResultSet rs) throws IOException, SQLException {
        // Output results.
        final FileSystem outFs = FileSystems.getFileSystem(outputFileChannelInstance.getSinglePath()).get();
        try (final OutputStreamWriter writer = new OutputStreamWriter(outFs.create(outputFileChannelInstance.getSinglePath()))) {
            while (rs.next()) {
                //System.out.println(rs.getInt(1) + " " + rs.getString(2));
                ResultSetMetaData rsmd = rs.getMetaData();
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    writer.write(rs.getString(i));
                    if (i < rsmd.getColumnCount()) {
                        writer.write('\t');
                    }
                }
                if (!rs.isLast()) {
                    writer.write('\n');
                }
            }
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }
}

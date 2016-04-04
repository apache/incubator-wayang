package org.qcri.rheem.postgres.execution;

import org.apache.commons.beanutils.converters.FloatConverter;
import org.apache.commons.lang3.Validate;
import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.platform.ExecutionProfile;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.util.fs.FileSystem;
import org.qcri.rheem.core.util.fs.FileSystems;
import org.qcri.rheem.postgres.PostgresPlatform;
import org.qcri.rheem.postgres.operators.PostgresTableSource;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.sql.*;
import java.util.Collection;

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

        Connection connection = platform.getConnection();
        PreparedStatement ps = null;

        try {
            ps = connection.prepareStatement("select * from " + tableOp.getTableName());
            rs = ps.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        final FileChannel outputFileChannel = (FileChannel) termTask.getOutputChannel(0);
        try {
            this.saveResult(outputFileChannel, rs);
            rs.close();
            ps.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
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
                System.out.println(rs.getInt(1) + " " + rs.getString(2));
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

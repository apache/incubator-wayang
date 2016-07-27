package org.qcri.rheem.jdbc.operators;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.OperatorBase;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.qcri.rheem.java.execution.JavaExecutor;
import org.qcri.rheem.java.operators.JavaExecutionOperator;
import org.qcri.rheem.jdbc.JdbcPlatformTemplate;
import org.qcri.rheem.jdbc.channels.SqlQueryChannel;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * This {@link Operator} converts {@link SqlQueryChannel}s to {@link StreamChannel}s.
 */
public class SqlToStreamOperator extends OperatorBase implements JavaExecutionOperator {

    public SqlToStreamOperator() {
        super(1, 1, false, null);
    }

    protected SqlToStreamOperator(OperatorBase that) {
        super(that);
    }

    @Override
    public void evaluate(ChannelInstance[] inputs, ChannelInstance[] outputs, JavaExecutor executor) {
        // Cast the inputs and outputs.
        final SqlQueryChannel.Instance input = (SqlQueryChannel.Instance) inputs[0];
        final StreamChannel.Instance output = (StreamChannel.Instance) outputs[0];

        JdbcPlatformTemplate producerPlatform = (JdbcPlatformTemplate) input.getChannel().getProducer().getPlatform();
        final Connection connection = producerPlatform
                .createDatabaseDescriptor(executor.getConfiguration())
                .createJdbcConnection();

        Iterator<Record> resultSetIterator = new ResultSetIterator(connection, input.getSqlQuery());
        Spliterator<Record> resultSetSpliterator = Spliterators.spliteratorUnknownSize(resultSetIterator, 0);
        Stream<Record> resultSetStream = StreamSupport.stream(resultSetSpliterator, false);

        output.accept(resultSetStream);
    }

    @Override
    public void evaluate(ChannelInstance[] inputs, ChannelInstance[] outputs, FunctionCompiler compiler) {
        throw new RheemException("This method should not be called.");
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return null;
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return null;
    }

    /**
     * Exposes a {@link ResultSet} as an {@link Iterator}.
     */
    private static class ResultSetIterator implements Iterator<Record>, AutoCloseable {

        /**
         * Keeps around the {@link ResultSet} of the SQL query.
         */
        private ResultSet resultSet;

        /**
         * The next {@link Record} to be delivered via {@link #next()}.
         */
        private Record next;

        /**
         * Creates a new instance.
         *
         * @param connection the JDBC connection on which to execute a SQL query
         * @param sqlQuery   the SQL query
         */
        ResultSetIterator(Connection connection, String sqlQuery) {
            try {
                this.resultSet = connection.createStatement().executeQuery(sqlQuery);
            } catch (SQLException e) {
                this.close();
                throw new RheemException("Could not execute SQL.", e);
            }
            this.moveToNext();
        }

        /**
         * Moves this instance to the next {@link Record}.
         */
        private void moveToNext() {
            try {
                if (this.resultSet == null || !this.resultSet.next()) {
                    this.next = null;
                    this.close();
                } else {
                    final int recordWidth = this.resultSet.getMetaData().getColumnCount();
                    Object[] values = new Object[recordWidth];
                    for (int i = 0; i < recordWidth; i++) {
                        values[i] = this.resultSet.getObject(i + 1);
                    }
                    this.next = new Record(values);
                }
            } catch (SQLException e) {
                this.next = null;
                this.close();
                throw new RheemException("Exception while iterating the result set.", e);
            }
        }

        @Override
        public boolean hasNext() {
            return this.next != null;
        }

        @Override
        public Record next() {
            Record curNext = this.next;
            this.moveToNext();
            return curNext;
        }

        @Override
        public void close() {
            if (this.resultSet != null) {
                try {
                    this.resultSet.close();
                } catch (Throwable t) {
                    LoggerFactory.getLogger(this.getClass()).error("Could not close result set.", t);
                } finally {
                    this.resultSet = null;
                }
            }
        }
    }


}

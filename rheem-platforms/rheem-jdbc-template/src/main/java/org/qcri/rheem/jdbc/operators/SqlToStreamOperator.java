package org.qcri.rheem.jdbc.operators;

import org.json.JSONObject;
import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.types.RecordType;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimators;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.UnaryToUnaryOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.JsonSerializable;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.execution.JavaExecutor;
import org.qcri.rheem.java.operators.JavaExecutionOperator;
import org.qcri.rheem.jdbc.channels.SqlQueryChannel;
import org.qcri.rheem.jdbc.platform.JdbcPlatformTemplate;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * This {@link Operator} converts {@link SqlQueryChannel}s to {@link StreamChannel}s.
 */
public class SqlToStreamOperator extends UnaryToUnaryOperator<Record, Record> implements JavaExecutionOperator, JsonSerializable {

    private final JdbcPlatformTemplate jdbcPlatform;

    /**
     * Creates a new instance.
     *
     * @param jdbcPlatform from which the SQL data comes
     */
    public SqlToStreamOperator(JdbcPlatformTemplate jdbcPlatform) {
        this(jdbcPlatform, DataSetType.createDefault(Record.class));
    }

    /**
     * Creates a new instance.
     *
     * @param jdbcPlatform from which the SQL data comes
     * @param dataSetType  type of the {@link Record}s being transformed; see {@link RecordType}
     */
    public SqlToStreamOperator(JdbcPlatformTemplate jdbcPlatform, DataSetType<Record> dataSetType) {
        super(dataSetType, dataSetType, false);
        this.jdbcPlatform = jdbcPlatform;
    }

    protected SqlToStreamOperator(SqlToStreamOperator that) {
        super(that);
        this.jdbcPlatform = that.jdbcPlatform;
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor executor,
            OptimizationContext.OperatorContext operatorContext) {
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

        ExecutionLineageNode queryLineageNode = new ExecutionLineageNode(operatorContext);
        queryLineageNode.add(LoadProfileEstimators.createFromSpecification(
                String.format("rheem.%s.sqltostream.load.query", this.jdbcPlatform.getPlatformId()),
                        executor.getConfiguration()
                ));
        queryLineageNode.addPredecessor(input.getLineage());
        ExecutionLineageNode outputLineageNode = new ExecutionLineageNode(operatorContext);
        outputLineageNode.add(LoadProfileEstimators.createFromSpecification(
                String.format("rheem.%s.sqltostream.load.output", this.jdbcPlatform.getPlatformId()),
                executor.getConfiguration()
        ));
        output.getLineage().addPredecessor(outputLineageNode);

        return queryLineageNode.collectAndMark();
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(this.jdbcPlatform.getSqlQueryChannelDescriptor());
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }

    @Override
    public Collection<String> getLoadProfileEstimatorConfigurationKeys() {
        return Arrays.asList(
                String.format("rheem.%s.sqltostream.load.query", this.jdbcPlatform.getPlatformId()),
                String.format("rheem.%s.sqltostream.load.output", this.jdbcPlatform.getPlatformId())
        );
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
                //connection.setAutoCommit(false);
                Statement st = connection.createStatement();
                //st.setFetchSize(100000000);
                this.resultSet = st.executeQuery(sqlQuery);
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

    @Override
    public JSONObject toJson() {
        return new JSONObject().put("platform", this.jdbcPlatform.getClass().getCanonicalName());
    }

    @SuppressWarnings("unused")
    public static SqlToStreamOperator fromJson(JSONObject jsonObject) {
        final String platformClassName = jsonObject.getString("platform");
        JdbcPlatformTemplate jdbcPlatform = ReflectionUtils.evaluate(platformClassName + ".getInstance()");
        return new SqlToStreamOperator(jdbcPlatform);
    }
}

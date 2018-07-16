package com.impetus.eth.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.blkch.BlkchnException;
import com.impetus.blkch.jdbc.AbstractPreparedStatement;
import com.impetus.blkch.sql.DataFrame;
import com.impetus.blkch.sql.parser.LogicalPlan;
import com.impetus.blkch.sql.query.FromItem;
import com.impetus.blkch.sql.query.IdentifierNode;
import com.impetus.blkch.sql.query.Table;
import com.impetus.blkch.util.placeholder.PlaceholderHandler;
import com.impetus.blkch.util.placeholder.QueryPlaceholderHandler;
import com.impetus.eth.parser.EthQueryExecutor;

public class EthPreparedStatement extends AbstractPreparedStatement {

    private static final Logger LOGGER = LoggerFactory.getLogger(EthPreparedStatement.class);

    protected EthConnection connection;

    protected int rSetType;

    protected int rSetConcurrency;

    protected String sql;

    private ResultSet queryResultSet = null;

    protected boolean isClosed = false;

    protected LogicalPlan logicalPlan;

    protected Object[] placeholderValues;

    protected PlaceholderHandler placeholderHandler;

    public EthPreparedStatement(EthConnection connection, String sql, int rSetType, int rSetConcurrency) {
        super();
        this.connection = connection;
        this.rSetType = rSetType;
        this.rSetConcurrency = rSetConcurrency;
        this.sql = sql;
        this.logicalPlan = getLogicalPlan(sql);

        switch (logicalPlan.getType()) {
            case INSERT:
                System.out.println("hello");
            default:
                placeholderHandler = new QueryPlaceholderHandler(logicalPlan);
        }
        placeholderHandler.setPlaceholderIndex();
        if (!placeholderHandler.isIndexListEmpty())
            this.placeholderValues = new Object[placeholderHandler.getIndexListCount()];
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        LOGGER.info("Entering into executeQuery Block");
        if (isClosed)
            throw new BlkchnException("No operations allowed after statement closed.");

        if (!placeholderHandler.isIndexListEmpty())
            placeholderHandler.alterLogicalPlan(placeholderValues);

        Object result = null;

        switch (logicalPlan.getType()) {
            case INSERT:
                result = new EthQueryExecutor(logicalPlan, connection.getWeb3jClient(), connection.getInfo())
                        .executeAndReturn();
                LOGGER.info("Exiting from execute Block with result: " + result);
                queryResultSet = new EthResultSet(result, rSetType, rSetConcurrency);
                LOGGER.info("Exiting from executeQuery Block");
                return queryResultSet;
            default:
                Table table = logicalPlan.getQuery().getChildType(FromItem.class, 0).getChildType(Table.class, 0);
                String tableName = table.getChildType(IdentifierNode.class, 0).getValue();
                DataFrame dataframe = new EthQueryExecutor(logicalPlan, connection.getWeb3jClient(),
                        connection.getInfo()).executeQuery();
                queryResultSet = new EthResultSet(dataframe, rSetType, rSetConcurrency, tableName);
                LOGGER.info("Exiting from executeQuery Block");
                return queryResultSet;
        }
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        if (parameterIndex <= placeholderValues.length)
            placeholderValues[parameterIndex - 1] = x;
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (parameterIndex <= placeholderValues.length)
            placeholderValues[parameterIndex - 1] = x;
        else
            throw new BlkchnException("Array index out of bound");
    }

    @Override
    public void close() throws SQLException {
        realClose();
    }

    private void realClose() throws SQLException {
        System.out.println("hello");
        if (isClosed)
            return;
        try {
            this.connection = null;
            this.isClosed = true;
            if (queryResultSet != null)
                queryResultSet.close();
            this.queryResultSet = null;
            this.rSetType = 0;
            this.rSetConcurrency = 0;
        } catch (Exception e) {
            throw new BlkchnException("Error while closing statement", e);
        }
    }

}

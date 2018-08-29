/******************************************************************************* 
* * Copyright 2018 Impetus Infotech.
* *
* * Licensed under the Apache License, Version 2.0 (the "License");
* * you may not use this file except in compliance with the License.
* * You may obtain a copy of the License at
* *
* * http://www.apache.org/licenses/LICENSE-2.0
* *
* * Unless required by applicable law or agreed to in writing, software
* * distributed under the License is distributed on an "AS IS" BASIS,
* * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* * See the License for the specific language governing permissions and
* * limitations under the License.
******************************************************************************/
package com.impetus.eth.jdbc;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.blkch.BlkchnException;
import com.impetus.blkch.jdbc.AbstractPreparedStatement;
import com.impetus.blkch.sql.DataFrame;
import com.impetus.blkch.sql.parser.LogicalPlan;
import com.impetus.blkch.sql.parser.LogicalPlan.SQLType;
import com.impetus.blkch.sql.query.FromItem;
import com.impetus.blkch.sql.query.IdentifierNode;
import com.impetus.blkch.sql.query.Table;
import com.impetus.blkch.util.placeholder.InsertPlaceholderHandler;
import com.impetus.blkch.util.placeholder.PlaceholderHandler;
import com.impetus.blkch.util.placeholder.QueryPlaceholderHandler;
import com.impetus.eth.parser.EthQueryExecutor;

import jnr.ffi.Struct.int16_t;

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

    protected int rowCount;
    
    protected int fetchSize;
    
    protected List<Object[]> batchList= new ArrayList<Object[]>();

    public EthPreparedStatement(EthConnection connection, String sql, int rSetType, int rSetConcurrency) {
        super();
        this.connection = connection;
        this.rSetType = rSetType;
        this.rSetConcurrency = rSetConcurrency;
        this.sql = sql;
        this.logicalPlan = getLogicalPlan(sql);

        if (logicalPlan.getType() == SQLType.INSERT) {
            placeholderHandler = new InsertPlaceholderHandler(logicalPlan);
        } else if (logicalPlan.getType() == SQLType.QUERY) {
            placeholderHandler = new QueryPlaceholderHandler(logicalPlan);
        } else {
            LOGGER.error("ERROR : Unknown Query Type ");
            throw new BlkchnException("ERROR : Unknown Query Type ");
        }

        placeholderHandler.setPlaceholderIndex();
        if (!placeholderHandler.isIndexListEmpty())
            this.placeholderValues = new Object[placeholderHandler.getIndexListCount()];
        //System.out.println("Sql is "+sql);
    }

    @Override
    public void addBatch() throws SQLException {
        if(logicalPlan.getType() == SQLType.QUERY)
            throw new BlkchnException("BlkchnStatement Batch can only contains insert or call statements !!");
        batchList.add(placeholderValues);
        /*It will clear the previous values*/
        //this.placeholderValues=new Object[placeholderHandler.getIndexListCount()];
        this.placeholderValues=placeholderValues.clone();
    }

    public List<Object[]> getBatchedArgs() {
        return this.batchList == null ? null : Collections.unmodifiableList(this.batchList);
    }
    
    @Override
    public int[] executeBatch() throws SQLException {
        LOGGER.info("Entering into executeBatch Block");
        if (isClosed)
            throw new BlkchnException("No operations allowed after statement closed.");
        if (this.batchList == null || this.batchList.size() == 0) {
            return new int[0];
        }
        int[] updateCounts= new int[batchList.size()];
        Arrays.fill(updateCounts, EXECUTE_FAILED);
        for(int i=0;i<batchList.size();i++){
            this.placeholderValues=batchList.get(i);
            try{
                updateCounts[i]=executeUpdate();
            }catch (BlkchnException e){
                updateCounts[i]=EXECUTE_FAILED;
                LOGGER.warn("PreparedStatement with values " + batchList.get(i).toString() + " gave exception "+e.getMessage());
            }
        }
        batchList.clear();
        return updateCounts;
    }
    @Override
    public ResultSet executeQuery() throws SQLException {
        LOGGER.info("Entering into executeQuery Block");
        if (isClosed)
            throw new BlkchnException("No operations allowed after statement closed.");

        if (!placeholderHandler.isIndexListEmpty())
            placeholderHandler.alterLogicalPlan(placeholderValues);

        switch (logicalPlan.getType()) {
            case QUERY:
                Table table = logicalPlan.getQuery().getChildType(FromItem.class, 0).getChildType(Table.class, 0);
                String tableName = table.getChildType(IdentifierNode.class, 0).getValue();
                EthQueryExecutor executor= new EthQueryExecutor(logicalPlan, connection.getWeb3jClient(),
                        connection.getInfo());
                DataFrame dataframe =executor.executeQuery();
                Map<String, Integer> dataTypeColumnMap = executor.computeDataTypeColumnMap();

                queryResultSet = new EthResultSet(dataframe, rSetType, rSetConcurrency, tableName,dataTypeColumnMap);
                LOGGER.info("Exiting from executeQuery Block");
                return queryResultSet;
            default:
                LOGGER.error("ERROR : Only SELECT Query is supported in this method");
                throw new BlkchnException("ERROR : Only SELECT Query is supported in this method");
        }
    }

    @Override
    public boolean execute() throws SQLException {
        LOGGER.error("ERROR : Method not supported");
        throw new BlkchnException("ERROR : Method not supported");
    }

    @Override
    public int executeUpdate() throws SQLException {
        LOGGER.info("Entering into executeUpdate Block");
        if (isClosed)
            throw new BlkchnException("No operations allowed after statement closed.");

        rowCount = 0;

        if (!placeholderHandler.isIndexListEmpty())
            placeholderHandler.alterLogicalPlan(placeholderValues);

        Object result = null;
        switch (logicalPlan.getType()) {
            case INSERT:
                result = new EthQueryExecutor(logicalPlan, connection.getWeb3jClient(), connection.getInfo())
                        .executeAndReturn();
                rowCount++;
                LOGGER.info("Exiting from execute Block with result: " + result);
                LOGGER.info("Exiting from executeUpdate Block");
                return rowCount;
            default:
                LOGGER.error("ERROR : Only INSERT Query is supported in this method");
                throw new BlkchnException("ERROR : Only INSERT Query is supported in this method");
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
        else {
            LOGGER.error("ERROR : Array index out of bound");
            throw new BlkchnException("Array index out of bound");
        }
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        if (parameterIndex <= placeholderValues.length)
            placeholderValues[parameterIndex - 1] = x;
        else {
            LOGGER.error("ERROR : Array index out of bound");
            throw new BlkchnException("Array index out of bound");
        }
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        if (parameterIndex <= placeholderValues.length)
            placeholderValues[parameterIndex - 1] = x;
        else {
            LOGGER.error("ERROR : Array index out of bound");
            throw new BlkchnException("Array index out of bound");
        }
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        if (parameterIndex <= placeholderValues.length)
            placeholderValues[parameterIndex - 1] = x;
        else {
            LOGGER.error("ERROR : Array index out of bound");
            throw new BlkchnException("Array index out of bound");
        }
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        if (parameterIndex <= placeholderValues.length)
            placeholderValues[parameterIndex - 1] = x;
        else {
            LOGGER.error("ERROR : Array index out of bound");
            throw new BlkchnException("Array index out of bound");
        }
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        if (parameterIndex <= placeholderValues.length)
            placeholderValues[parameterIndex - 1] = x;
        else {
            LOGGER.error("ERROR : Array index out of bound");
            throw new BlkchnException("Array index out of bound");
        }
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        if (parameterIndex <= placeholderValues.length)
            placeholderValues[parameterIndex - 1] = x;
        else {
            LOGGER.error("ERROR : Array index out of bound");
            throw new BlkchnException("Array index out of bound");
        }
    }

    @Override
    public void clearParameters() throws SQLException {
        if (!placeholderHandler.isIndexListEmpty())
            this.placeholderValues = new Object[placeholderHandler.getIndexListCount()];
    }
    
    @Override
    public void setFetchSize(int rows) throws SQLException {
       this.fetchSize=rows;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.isClosed;
    }

    @Override
    public void close() throws SQLException {
        realClose();
    }

    private void realClose() throws SQLException {
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
            batchList.clear();
        } catch (Exception e) {
            LOGGER.error("Error while closing statement");
            throw new BlkchnException("Error while closing statement", e);
        }
    }

}

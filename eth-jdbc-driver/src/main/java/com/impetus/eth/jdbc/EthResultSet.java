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
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

import com.impetus.blkch.BlkchnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.blkch.jdbc.AbstractResultSet;
import com.impetus.blkch.sql.DataFrame;

/**
 * The Class EthResultSet.
 * 
 * @author karthikp.manchala
 * 
 */
public class EthResultSet extends AbstractResultSet {

    private static final Logger LOGGER = LoggerFactory.getLogger(EthResultSet.class);

    protected static final int BEFORE_FIRST_ROW = 0;

    protected static final int AFTER_LAST_ROW = -1;

    /** Has this ResultSet been closed? */
    protected boolean isClosed = false;

    protected int currentRowCursor;

    protected int totalRowCount;

    protected List<List<Object>> rowData;

    protected Object[] currentRow;

    protected Map<String, Integer> columnNamesMap;

    protected int resultSetType;

    protected int rSetConcurrency;

    protected String tableName;

    protected Map<String, String> aliasMapping;

    protected Map<String, Integer> colTypeMap;

    private static final String EXCEPTION_MSG = "Result set doesn't contain index %d";

    public EthResultSet(DataFrame dataframe, int resultSetType, int rSetConcurrency, String tableName) {
        LOGGER.info("Instantiating new Result Set ");
        this.rowData = dataframe.getData();
        this.columnNamesMap = dataframe.getColumnNamesMap();
        this.resultSetType = resultSetType;
        this.rSetConcurrency = rSetConcurrency;
        this.tableName = tableName;
        this.aliasMapping = dataframe.getAliasMapping();
        currentRowCursor = BEFORE_FIRST_ROW;
        totalRowCount = rowData.size();
    }

    public EthResultSet(DataFrame dataframe, int resultSetType, int rSetConcurrency, String tableName,
        Map<String, Integer> colTypeMap) {
        LOGGER.info("Instantiating new Result Set ");
        this.rowData = dataframe.getData();
        this.columnNamesMap = dataframe.getColumnNamesMap();
        this.resultSetType = resultSetType;
        this.rSetConcurrency = rSetConcurrency;
        this.tableName = tableName;
        this.aliasMapping = dataframe.getAliasMapping();
        this.colTypeMap = colTypeMap;
        currentRowCursor = BEFORE_FIRST_ROW;
        totalRowCount = rowData.size();
    }

    public EthResultSet(Object data, int resultSetType, int rSetConcurrency) {
        LOGGER.info("Instantiating new Result Set ");
        this.rowData = new ArrayList<>();
        this.rowData.add(Arrays.asList(data));
        this.columnNamesMap = new HashMap<>();
        this.columnNamesMap.put("Receipt", 1);
        this.resultSetType = resultSetType;
        this.rSetConcurrency = rSetConcurrency;
        this.colTypeMap = new HashMap<>();
        //Receipt 2000 for SQL JDBC_OBJECT type
        colTypeMap.put("Receipt",2000);
        this.tableName = "TransactionReceipt";
        currentRowCursor = BEFORE_FIRST_ROW;
        totalRowCount = rowData.size();
    }

    @Override
    public void close() throws SQLException {
        realClose();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.isClosed;
    }

    private void realClose() throws SQLException {
        if (isClosed)
            return;
        try {
            this.isClosed = true;
            this.currentRowCursor = 0;
            this.totalRowCount = 0;
            this.rowData = null;
            this.currentRow = null;
            this.columnNamesMap = null;
            this.resultSetType = 0;
            this.rSetConcurrency = 0;
            this.tableName = null;
            this.aliasMapping = null;
        } catch (Exception e) {
            throw new BlkchnException("Error while closing ResultSet", e);
        }
    }

    protected final void checkClosed() throws SQLException {
        if (this.isClosed) {
            throw new BlkchnException("ResultSet is already closed.");
        }
    }

    @Override
    public boolean first() throws SQLException {
        checkClosed();
        LOGGER.info("Moving the cursor to the first row of ResultSet Object");
        checkRSForward();
        if (totalRowCount > 0) {
            currentRowCursor = 1;
            currentRow = rowData.get(currentRowCursor - 1).toArray();
            return true;
        }
        return false;
    }

    @Override
    public boolean last() throws SQLException {
        checkClosed();
        LOGGER.info("Moving the cursor to the last row of ResultSet Object");
        checkRSForward();
        if (totalRowCount > 0) {
            currentRowCursor = totalRowCount;
            currentRow = rowData.get(currentRowCursor - 1).toArray();
            return true;
        }
        return false;
    }

    @Override
    public boolean next() throws SQLException {
        checkClosed();
        LOGGER.info("Moving the cursor to the next row of ResultSet Object");
        if (currentRowCursor != AFTER_LAST_ROW && currentRowCursor < totalRowCount) {
            currentRowCursor++;
            currentRow = rowData.get(currentRowCursor - 1).toArray();
            return true;
        } else {
            currentRowCursor = AFTER_LAST_ROW;
            return false;
        }
    }

    @Override
    public boolean previous() throws SQLException {
        checkClosed();
        LOGGER.info("Moving the cursor to the previous row of ResultSet Object");
        checkRSForward();
        if (currentRowCursor > 1) {
            currentRowCursor--;
            currentRow = rowData.get(currentRowCursor - 1).toArray();
            return true;
        } else {
            currentRowCursor = BEFORE_FIRST_ROW;
            return false;
        }
    }

    @Override
    public void afterLast() throws SQLException {
        checkClosed();
        LOGGER.info("Moving the cursor to thee end of ResultSet Object");
        checkRSForward();
        currentRowCursor = AFTER_LAST_ROW;
    }

    @Override
    public void beforeFirst() throws SQLException {
        checkClosed();
        LOGGER.info("Moving the cursor to the front of ResultSet Object");
        checkRSForward();
        currentRowCursor = BEFORE_FIRST_ROW;
    }

    @Override
    public boolean isFirst() throws SQLException {
        checkClosed();
        LOGGER.info("Checking if cursor is on the first row of result set object");
        if (currentRowCursor == 1)
            return true;
        else
            return false;
    }

    @Override
    public boolean isLast() throws SQLException {
        checkClosed();
        LOGGER.info("Checking if cursor is on the last row of result set object");
        if (currentRowCursor == totalRowCount)
            return true;
        else
            return false;
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        checkRSForward();

        if (rows == 0)
            return true;

        if (rows == -1)
            return previous();

        if (rows == 1)
            return next();

        if ((rows < 0 && (currentRowCursor + rows) > 0) || (rows > 0 && (currentRowCursor + rows) <= totalRowCount)) {
            currentRowCursor = currentRowCursor + rows;
            currentRow = rowData.get(currentRowCursor - 1).toArray();
            return true;
        } else
            return false;

    }

    @Override
    public int getFetchSize() throws SQLException {
        LOGGER.info("Calculating Result set fetch size");
        return totalRowCount;
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        int idx = getColumnIndex(columnLabel);
        if (currentRow[idx] instanceof BigInteger) {
            return ((BigInteger) currentRow[idx]).toString();
        }
        return (String) currentRow[idx];
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        checkClosed();
        if (columnIndex < 1 || columnIndex > currentRow.length) {
            throw new SQLException(String.format(EXCEPTION_MSG, columnIndex));
        }
        if (currentRow[columnIndex - 1] instanceof BigInteger) {
            return ((BigInteger) currentRow[columnIndex - 1]).toString();
        }
        return (String) currentRow[columnIndex - 1];
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return currentRow[getColumnIndex(columnLabel)];
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        checkClosed();
        if (columnIndex < 1 || columnIndex > currentRow.length) {
            throw new SQLException(String.format(EXCEPTION_MSG, columnIndex));
        }
        return currentRow[columnIndex - 1];
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        checkClosed();
        if (columnIndex < 1 || columnIndex > currentRow.length) {
            throw new SQLException(String.format(EXCEPTION_MSG, columnIndex));
        }
        return (int) currentRow[columnIndex - 1];
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return (int) currentRow[getColumnIndex(columnLabel)];
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        checkClosed();
        if (columnIndex < 1 || columnIndex > currentRow.length) {
            throw new SQLException(String.format(EXCEPTION_MSG, columnIndex));
        }
        return (long) currentRow[columnIndex - 1];
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return (long) currentRow[getColumnIndex(columnLabel)];
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        checkClosed();
        if (columnIndex < 1 || columnIndex > currentRow.length) {
            throw new SQLException(String.format(EXCEPTION_MSG, columnIndex));
        }
        if (currentRow[columnIndex - 1] instanceof BigInteger) {
            return new BigDecimal((BigInteger)currentRow[columnIndex - 1]);
        }
        return (BigDecimal) currentRow[columnIndex - 1];
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        if (currentRow[getColumnIndex(columnLabel)] instanceof BigInteger) {
            return new BigDecimal((BigInteger)currentRow[getColumnIndex(columnLabel)]);
        }
        return (BigDecimal) currentRow[getColumnIndex(columnLabel)];
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        checkClosed();
        if (columnIndex < 1 || columnIndex > currentRow.length) {
            throw new SQLException(String.format(EXCEPTION_MSG, columnIndex));
        }
        return (boolean) currentRow[columnIndex - 1];
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return (boolean) currentRow[getColumnIndex(columnLabel)];
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        checkClosed();
        if (columnIndex < 1 || columnIndex > currentRow.length) {
            throw new SQLException(String.format(EXCEPTION_MSG, columnIndex));
        }
        return (byte) currentRow[columnIndex - 1];
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return (byte) currentRow[getColumnIndex(columnLabel)];
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        checkClosed();
        if (columnIndex < 1 || columnIndex > currentRow.length) {
            throw new SQLException(String.format(EXCEPTION_MSG, columnIndex));
        }
        return (byte[]) currentRow[columnIndex - 1];
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return (byte[]) currentRow[getColumnIndex(columnLabel)];
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        checkClosed();
        if (columnIndex < 1 || columnIndex > currentRow.length) {
            throw new SQLException(String.format(EXCEPTION_MSG, columnIndex));
        }
        return (double) currentRow[columnIndex - 1];
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return (double) currentRow[getColumnIndex(columnLabel)];
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        checkClosed();
        if (columnIndex < 1 || columnIndex > currentRow.length) {
            throw new SQLException(String.format(EXCEPTION_MSG, columnIndex));
        }
        return (short) currentRow[columnIndex - 1];
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return (short) currentRow[getColumnIndex(columnLabel)];
    }

    @Override
    public int getRow() throws SQLException {
        return currentRowCursor;
    }

    @Override
    public int getType() throws SQLException {
        return resultSetType;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkClosed();
        return new EthResultSetMetaData(tableName, columnNamesMap, aliasMapping, colTypeMap);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return columnNamesMap.get(columnLabel);
    }

    protected void checkRSForward() throws SQLException {
        checkClosed();
        LOGGER.info("checking result set type ");
        if (resultSetType == ResultSet.TYPE_FORWARD_ONLY) {
            LOGGER.error("Result Set is Type Forward only. Exiting...");
            throw new SQLException("Result Set is Type Forward only", "1000");
        }
        LOGGER.info("Result set type validation Completed ");
    }

    protected int getColumnIndex(String columnLabel) {
        if (!aliasMapping.isEmpty() && aliasMapping.containsKey(columnLabel)) {
            return columnNamesMap.get(aliasMapping.get(columnLabel));
        } else {
            if (columnNamesMap.get(columnLabel) == null) {
                LOGGER.error("Column: " + columnLabel + " is not a part of query");
                throw new RuntimeException("Column: " + columnLabel + " is not a part of query");
            }
            return columnNamesMap.get(columnLabel);
        }
    }

}

/******************************************************************************* 
 * * Copyright 2017 Impetus Infotech.
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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.eth.parser.DataFrame;

/**
 * The Class EthResultSet.
 * 
 * @author karthikp.manchala
 * 
 */
public class EthResultSet extends AbstractResultSet
{
    
    /** The Constant LOGGER. */
    private static final Logger LOGGER = LoggerFactory.getLogger(EthResultSet.class);

    /** The Constant BEFORE_FIRST_ROW. */
    protected static final int BEFORE_FIRST_ROW = 0;

    /** The Constant AFTER_LAST_ROW. */
    protected static final int AFTER_LAST_ROW = -1;

    /** The current row cursor. */
    protected int currentRowCursor;

    /** The total row count. */
    protected int totalRowCount;

    /** The row data. */
    protected List<List<Object>> rowData;

    /** The current row. */
    protected Object[] currentRow;

    /** The column names map. */
    protected HashMap<String, Integer> columnNamesMap;

    /** The result set type. */
    protected int resultSetType;

    /** The r set concurrency. */
    protected int rSetConcurrency;

    /** The table name. */
    protected String tableName;
    
    protected Map<String,String> aliasMapping;

    /**
     * Instantiates a new eth result set.
     *
     * @param dataframe the dataframe
     * @param resultSetType            the result set type
     * @param rSetConcurrency            the r set concurrency
     */
    public EthResultSet(DataFrame dataframe, int resultSetType,
            int rSetConcurrency)
    {
        LOGGER.info("Instantiating new Result Set ");
        this.rowData = dataframe.getData();
        this.columnNamesMap = dataframe.getColumnNamesMap();
        this.resultSetType = resultSetType;
        this.rSetConcurrency = rSetConcurrency;
        this.tableName = dataframe.getTable();
        this.aliasMapping=dataframe.getAliasMapping();
        currentRowCursor = BEFORE_FIRST_ROW;
        totalRowCount = rowData.size();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.eth.jdbc.AbstractResultSet#first()
     */
    @Override
    public boolean first() throws SQLException
    {
        LOGGER.info("Moving the cursor to the first row of ResultSet Object");
        checkRSForward();
        if (totalRowCount > 0)
        {
            currentRowCursor = 1;
            currentRow = rowData.get(currentRowCursor - 1).toArray();
            return true;
        }
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.eth.jdbc.AbstractResultSet#last()
     */
    @Override
    public boolean last() throws SQLException
    {
        LOGGER.info("Moving the cursor to the last row of ResultSet Object");
        checkRSForward();
        if (totalRowCount > 0)
        {
            currentRowCursor = totalRowCount;
            currentRow = rowData.get(currentRowCursor - 1).toArray();
            return true;
        }
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.eth.jdbc.AbstractResultSet#next()
     */
    @Override
    public boolean next() throws SQLException
    {
        LOGGER.info("Moving the cursor to the next row of ResultSet Object");
        if (currentRowCursor != AFTER_LAST_ROW && currentRowCursor < totalRowCount)
        {
            currentRowCursor++;
            currentRow = rowData.get(currentRowCursor - 1).toArray();
            return true;
        }
        else
        {
            currentRowCursor = AFTER_LAST_ROW;
            return false;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.eth.jdbc.AbstractResultSet#previous()
     */
    @Override
    public boolean previous() throws SQLException
    {
        LOGGER.info("Moving the cursor to the previous row of ResultSet Object");
        checkRSForward();
        if (currentRowCursor > 1)
        {
            currentRowCursor--;
            currentRow = rowData.get(currentRowCursor - 1).toArray();
            return true;
        }
        else
        {
            currentRowCursor = BEFORE_FIRST_ROW;
            return false;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.eth.jdbc.AbstractResultSet#afterLast()
     */
    @Override
    public void afterLast() throws SQLException
    {
        LOGGER.info("Moving the cursor to thee end of ResultSet Object");
        checkRSForward();
        currentRowCursor = AFTER_LAST_ROW;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.eth.jdbc.AbstractResultSet#beforeFirst()
     */
    @Override
    public void beforeFirst() throws SQLException
    {
        LOGGER.info("Moving the cursor to the front of ResultSet Object");
        checkRSForward();
        currentRowCursor = BEFORE_FIRST_ROW;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.eth.jdbc.AbstractResultSet#isFirst()
     */
    @Override
    public boolean isFirst() throws SQLException
    {
        LOGGER.info("Checking if cursor is on the first row of result set object");
        if (currentRowCursor == 1)
            return true;
        else
            return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.eth.jdbc.AbstractResultSet#isLast()
     */
    @Override
    public boolean isLast() throws SQLException
    {
        LOGGER.info("Checking if cursor is on the last row of result set object");
        if (currentRowCursor == totalRowCount)
            return true;
        else
            return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.eth.jdbc.AbstractResultSet#relative(int)
     */
    @Override
    public boolean relative(int rows) throws SQLException
    {
        checkRSForward();

        if (rows == 0)
            return true;

        if (rows == -1)
            return previous();

        if (rows == 1)
            return next();

        if ((rows < 0 && (currentRowCursor + rows) > 0) || (rows > 0 && (currentRowCursor + rows) <= totalRowCount))
        {
            currentRowCursor = currentRowCursor + rows;
            currentRow = rowData.get(currentRowCursor - 1).toArray();
            return true;
        }
        else
            return false;

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.eth.jdbc.AbstractResultSet#getFetchSize()
     */
    @Override
    public int getFetchSize() throws SQLException
    {
        LOGGER.info("Calculating Result set fetch size");
        return totalRowCount;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.eth.jdbc.AbstractResultSet#getString(java.lang.String)
     */
    @Override
    public String getString(String columnLabel) throws SQLException
    {
        return (String) currentRow[getColumnIndex(columnLabel)];
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.eth.jdbc.AbstractResultSet#getString(int)
     */
    @Override
    public String getString(int columnIndex) throws SQLException
    {
        return (String) currentRow[columnIndex];
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.eth.jdbc.AbstractResultSet#getObject(java.lang.String)
     */
    @Override
    public Object getObject(String columnLabel) throws SQLException
    {
        return currentRow[getColumnIndex(columnLabel)];
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.eth.jdbc.AbstractResultSet#getObject(int)
     */
    @Override
    public Object getObject(int columnIndex) throws SQLException
    {
        return currentRow[columnIndex];
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.eth.jdbc.AbstractResultSet#getInt(int)
     */
    @Override
    public int getInt(int columnIndex) throws SQLException
    {
        return (int) currentRow[columnIndex];
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.eth.jdbc.AbstractResultSet#getInt(java.lang.String)
     */
    @Override
    public int getInt(String columnLabel) throws SQLException
    {
        return (int) currentRow[getColumnIndex(columnLabel)];
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.eth.jdbc.AbstractResultSet#getLong(int)
     */
    @Override
    public long getLong(int columnIndex) throws SQLException
    {
        return (long) currentRow[columnIndex];
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.eth.jdbc.AbstractResultSet#getLong(java.lang.String)
     */
    @Override
    public long getLong(String columnLabel) throws SQLException
    {
        return (long) currentRow[getColumnIndex(columnLabel)];
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.eth.jdbc.AbstractResultSet#getBigDecimal(int)
     */
    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException
    {
        return (BigDecimal) currentRow[columnIndex];
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.eth.jdbc.AbstractResultSet#getBigDecimal(java.lang.String)
     */
    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException
    {
        return (BigDecimal) currentRow[getColumnIndex(columnLabel)];
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.eth.jdbc.AbstractResultSet#getBoolean(int)
     */
    @Override
    public boolean getBoolean(int columnIndex) throws SQLException
    {
        return (boolean) currentRow[columnIndex];
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.eth.jdbc.AbstractResultSet#getBoolean(java.lang.String)
     */
    @Override
    public boolean getBoolean(String columnLabel) throws SQLException
    {
        return (boolean) currentRow[getColumnIndex(columnLabel)];
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.eth.jdbc.AbstractResultSet#getByte(int)
     */
    @Override
    public byte getByte(int columnIndex) throws SQLException
    {
        return (byte) currentRow[columnIndex];
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.eth.jdbc.AbstractResultSet#getByte(java.lang.String)
     */
    @Override
    public byte getByte(String columnLabel) throws SQLException
    {
        return (byte) currentRow[getColumnIndex(columnLabel)];
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.eth.jdbc.AbstractResultSet#getBytes(int)
     */
    @Override
    public byte[] getBytes(int columnIndex) throws SQLException
    {
        return (byte[]) currentRow[columnIndex];
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.eth.jdbc.AbstractResultSet#getBytes(java.lang.String)
     */
    @Override
    public byte[] getBytes(String columnLabel) throws SQLException
    {
        return (byte[]) currentRow[getColumnIndex(columnLabel)];
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.eth.jdbc.AbstractResultSet#getDouble(int)
     */
    @Override
    public double getDouble(int columnIndex) throws SQLException
    {
        return (double) currentRow[columnIndex];
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.eth.jdbc.AbstractResultSet#getDouble(java.lang.String)
     */
    @Override
    public double getDouble(String columnLabel) throws SQLException
    {
        return (double) currentRow[getColumnIndex(columnLabel)];
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.eth.jdbc.AbstractResultSet#getShort(int)
     */
    @Override
    public short getShort(int columnIndex) throws SQLException
    {
        return (short) currentRow[columnIndex];
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.eth.jdbc.AbstractResultSet#getShort(java.lang.String)
     */
    @Override
    public short getShort(String columnLabel) throws SQLException
    {
        return (short) currentRow[getColumnIndex(columnLabel)];
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.eth.jdbc.AbstractResultSet#getRow()
     */
    @Override
    public int getRow() throws SQLException
    {
        return currentRowCursor;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.eth.jdbc.AbstractResultSet#getType()
     */
    @Override
    public int getType() throws SQLException
    {
        return resultSetType;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.eth.jdbc.AbstractResultSet#getMetaData()
     */
    @Override
    public ResultSetMetaData getMetaData() throws SQLException
    {
        return new EthResultSetMetaData(tableName, columnNamesMap,aliasMapping);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.eth.jdbc.AbstractResultSet#findColumn(java.lang.String)
     */
    @Override
    public int findColumn(String columnLabel) throws SQLException
    {
        return columnNamesMap.get(columnLabel);
    }

    /**
     * Check RS forward.
     *
     * @throws SQLException
     *             the SQL exception
     */
    protected void checkRSForward() throws SQLException
    {
        LOGGER.info("checking result set type ");
        if (resultSetType == ResultSet.TYPE_FORWARD_ONLY)
        {
            LOGGER.error("Result Set is Type Forward only. Exiting...");
            throw new SQLException("Result Set is Type Forward only", "1000");
        }
        LOGGER.info("Result set type validation Completed ");
    }

    protected int getColumnIndex(String columnLabel){
        if(!aliasMapping.isEmpty()&&aliasMapping.containsKey(columnLabel)){
            return columnNamesMap.get(aliasMapping.get(columnLabel));
        }else
            return columnNamesMap.get(columnLabel);
        
    }

}

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

import java.io.IOException;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.List;

import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.core.methods.response.EthBlock.Block;
import org.web3j.protocol.core.methods.response.EthBlock.TransactionResult;
import org.web3j.protocol.core.methods.response.Transaction;

import com.impetus.blkch.jdbc.BlkchnStatement;

/**
 * The Class EthStatement.
 * 
 * @author ashishk.shukla
 * 
 */
public class EthStatement implements BlkchnStatement
{

    /** The connection. */
    protected EthConnection connection;

    /** The r set type. */
    protected int rSetType;

    /** The r set concurrency. */
    protected int rSetConcurrency;

    /**
     * Gets the r set type.
     *
     * @return the r set type
     */
    public int getrSetType()
    {
        return rSetType;
    }

    /**
     * Sets the r set type.
     *
     * @param rSetType
     *            the new r set type
     */
    public void setrSetType(int rSetType)
    {
        this.rSetType = rSetType;
    }

    /**
     * Gets the r set concurrency.
     *
     * @return the r set concurrency
     */
    public int getrSetConcurrency()
    {
        return rSetConcurrency;
    }

    /**
     * Sets the r set concurrency.
     *
     * @param rSetConcurrency
     *            the new r set concurrency
     */
    public void setrSetConcurrency(int rSetConcurrency)
    {
        this.rSetConcurrency = rSetConcurrency;
    }

    /**
     * Sets the connection.
     *
     * @param connection
     *            the new connection
     */
    public void setConnection(EthConnection connection)
    {
        this.connection = connection;
    }

    /**
     * Instantiates a new eth statement.
     *
     * @param connection
     *            the connection
     * @param rSetType
     *            the r set type
     * @param rSetConcurrency
     *            the r set concurrency
     */
    public EthStatement(EthConnection connection, int rSetType, int rSetConcurrency)
    {
        super();
        this.connection = connection;
        this.rSetType = rSetType;
        this.rSetConcurrency = rSetConcurrency;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
     */
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Wrapper#unwrap(java.lang.Class)
     */
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#addBatch(java.lang.String)
     */
    @Override
    public void addBatch(String sql) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#cancel()
     */
    @Override
    public void cancel() throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#clearBatch()
     */
    @Override
    public void clearBatch() throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#clearWarnings()
     */
    @Override
    public void clearWarnings() throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#close()
     */
    @Override
    public void close() throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#closeOnCompletion()
     */
    @Override
    public void closeOnCompletion() throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#execute(java.lang.String)
     */
    @Override
    public boolean execute(String sql) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#execute(java.lang.String, int)
     */
    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#execute(java.lang.String, int[])
     */
    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#execute(java.lang.String, java.lang.String[])
     */
    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#executeBatch()
     */
    @Override
    public int[] executeBatch() throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#executeQuery(java.lang.String)
     */
    @Override
    public ResultSet executeQuery(String sql) throws SQLException
    {

        ResultSet queryResultSet = null;
        try
        {
           
            List<TransactionResult> trans = getTransactions("1876545");
            TransactionResultDataHandler dataHandler = new TransactionResultDataHandler();
            queryResultSet = new EthResultSet(dataHandler.convertToObjArray(trans), dataHandler.getColumnNamesMap(),
                    rSetType, rSetConcurrency, dataHandler.getTableName());

            /*
             * Block blk= getBlock("1876545"); List<Block> blkl= new
             * ArrayList<Block>(); blkl.add(blk); BlockResultDataHandler
             * blockDataHandler= new BlockResultDataHandler(); queryResultSet=
             * new EthResultSet(blockDataHandler.convertToObjArray(blkl),
             * BlockResultDataHandler
             * .getColumnNamesMap(),rSetType,rSetConcurrency
             * ,blockDataHandler.getTableName());
             */

            /*
             * Block blkByHash= getBlockByHash(
             * "0xb76e315ce8a2531e910b2c55d975579d1b05b451eed46947d98278f18f22d25d"
             * ); List<Block> lblkByHash= new ArrayList<Block>();
             * blkl.add(blkByHash); BlockResultDataHandler blkByHashDataHandler=
             * new BlockResultDataHandler(); queryResultSet= new
             * EthResultSet(blkByHashDataHandler.convertToObjArray(blkl),
             * BlockResultDataHandler
             * .getColumnNamesMap(),rSetType,rSetConcurrency
             * ,blkByHashDataHandler.getTableName());
             * 
             * 
             * Transaction transByHash= getTransactionByHash(
             * "0xe74e6119caa2f92ca50fee714c8ded39385c1ede944aad60dbf3a2cfc69d5b23"
             * ); List<Transaction> lTransByHash= new ArrayList<Transaction>();
             * lTransByHash.add(transByHash); TransactionResultDataHandler
             * transByHashdataHandler = new TransactionResultDataHandler();
             * queryResultSet = new
             * EthResultSet(transByHashdataHandler.convertToObjArray
             * (lTransByHash),
             * transByHashdataHandler.getColumnNamesMap(),rSetType
             * ,rSetConcurrency,transByHashdataHandler.getTableName());
             */

        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        return queryResultSet;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#executeUpdate(java.lang.String)
     */
    @Override
    public int executeUpdate(String sql) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#executeUpdate(java.lang.String, int)
     */
    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#executeUpdate(java.lang.String, int[])
     */
    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#executeUpdate(java.lang.String,
     * java.lang.String[])
     */
    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getConnection()
     */
    @Override
    public Connection getConnection() throws SQLException
    {

        return connection;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getFetchDirection()
     */
    @Override
    public int getFetchDirection() throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getFetchSize()
     */
    @Override
    public int getFetchSize() throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getGeneratedKeys()
     */
    @Override
    public ResultSet getGeneratedKeys() throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getMaxFieldSize()
     */
    @Override
    public int getMaxFieldSize() throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getMaxRows()
     */
    @Override
    public int getMaxRows() throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getMoreResults()
     */
    @Override
    public boolean getMoreResults() throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getMoreResults(int)
     */
    @Override
    public boolean getMoreResults(int current) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getQueryTimeout()
     */
    @Override
    public int getQueryTimeout() throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getResultSet()
     */
    @Override
    public ResultSet getResultSet() throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getResultSetConcurrency()
     */
    @Override
    public int getResultSetConcurrency() throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getResultSetHoldability()
     */
    @Override
    public int getResultSetHoldability() throws SQLException
    {
        throw new UnsupportedOperationException();

    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getResultSetType()
     */
    @Override
    public int getResultSetType() throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getUpdateCount()
     */
    @Override
    public int getUpdateCount() throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getWarnings()
     */
    @Override
    public SQLWarning getWarnings() throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#isCloseOnCompletion()
     */
    @Override
    public boolean isCloseOnCompletion() throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#isClosed()
     */
    @Override
    public boolean isClosed() throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#isPoolable()
     */
    @Override
    public boolean isPoolable() throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#setCursorName(java.lang.String)
     */
    @Override
    public void setCursorName(String name) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#setEscapeProcessing(boolean)
     */
    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#setFetchDirection(int)
     */
    @Override
    public void setFetchDirection(int direction) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#setFetchSize(int)
     */
    @Override
    public void setFetchSize(int rows) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#setMaxFieldSize(int)
     */
    @Override
    public void setMaxFieldSize(int max) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#setMaxRows(int)
     */
    @Override
    public void setMaxRows(int max) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#setPoolable(boolean)
     */
    @Override
    public void setPoolable(boolean poolable) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#setQueryTimeout(int)
     */
    @Override
    public void setQueryTimeout(int seconds) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the transactions.
     *
     * @param blockNumber
     *            the block number
     * @return the transactions
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private List<TransactionResult> getTransactions(String blockNumber) throws IOException
    {
        EthBlock block = connection.getWeb3jClient()
                .ethGetBlockByNumber(DefaultBlockParameter.valueOf(new BigInteger(blockNumber)), true).send();

        return block.getBlock().getTransactions();
    }

    /**
     * Gets the block.
     *
     * @param blockNumber
     *            the block number
     * @return the block
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private Block getBlock(String blockNumber) throws IOException
    {
        EthBlock block = connection.getWeb3jClient()
                .ethGetBlockByNumber(DefaultBlockParameter.valueOf(new BigInteger(blockNumber)), true).send();
        return block.getBlock();
    }

    /**
     * Gets the block by hash.
     *
     * @param blockHash
     *            the block hash
     * @return the block by hash
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private Block getBlockByHash(String blockHash) throws IOException
    {
        EthBlock block = connection.getWeb3jClient().ethGetBlockByHash(blockHash, true).send();
        return block.getBlock();
    }

    /**
     * Gets the transaction by hash.
     *
     * @param transactionHash
     *            the transaction hash
     * @return the transaction by hash
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private Transaction getTransactionByHash(String transactionHash) throws IOException
    {
        Transaction transaction = connection.getWeb3jClient().ethGetTransactionByHash(transactionHash).send()
                .getResult();
        return transaction;
    }

    /**
     * Gets the transaction by block hash and index.
     *
     * @param blockHash
     *            the block hash
     * @param transactionIndex
     *            the transaction index
     * @return the transaction by block hash and index
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private Transaction getTransactionByBlockHashAndIndex(String blockHash, BigInteger transactionIndex)
            throws IOException
    {
        Transaction transaction = connection.getWeb3jClient()
                .ethGetTransactionByBlockHashAndIndex(blockHash, transactionIndex).send().getResult();
        return transaction;
    }

    /**
     * Gets the test.
     *
     * @param blockHash
     *            the block hash
     * @param transactionIndex
     *            the transaction index
     * @return the test
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private Transaction gettest(String blockHash, BigInteger transactionIndex) throws IOException
    {

        // Transaction transaction = connection.getWeb3jClient().
        // ethGetTransactionCount("", null);
        // ethGetUncleCountByBlockNumber(defaultBlockParameter)
        // ethGetUncleByBlockNumberAndIndex(defaultBlockParameter,
        // transactionIndex)
        // ethGetUncleCountByBlockHash(blockHash)
        // ethGetUncleByBlockHashAndIndex(blockHash, transactionIndex)
        // ethGetTransactionCount(address, defaultBlockParameter)
        // ethGetTransactionByBlockNumberAndIndex(defaultBlockParameter,
        // transactionIndex)
        // ethGetTransactionByBlockHashAndIndex(blockHash, transactionIndex)
        // ethGetBlockTransactionCountByNumber(defaultBlockParameter)
        // ethGetBlockTransactionCountByHash(blockHash)
        return null;
    }

}

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

import java.lang.ref.WeakReference;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import com.impetus.blkch.BlkchnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.http.HttpService;
import org.web3j.protocol.ipc.UnixIpcService;
import org.web3j.protocol.ipc.WindowsIpcService;

import com.impetus.blkch.jdbc.BlkchnConnection;

/**
 * The Class EthConnection.
 * 
 * @author ashishk.shukla
 * 
 */
public class EthConnection implements BlkchnConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(EthConnection.class);

    private String url;

    private Properties props;

    private Web3j web3jClient;

    private ArrayList statementList = new ArrayList();

    /** Has this connection been closed? */
    protected boolean isClosed = false;

    public Web3j getWeb3jClient() {
        return web3jClient;
    }

    public void setWeb3jClient(Web3j web3jClient) {
        this.web3jClient = web3jClient;
    }

    public void addNewStatement(EthStatement statement) {
        synchronized (statementList) {
            for (int i = 0; i < statementList.size(); i++) {
                WeakReference wr = (WeakReference) statementList.get(i);
                if (wr == null || wr.get() == null) {
                    statementList.set(i, new WeakReference(statement));
                    return;
                }
            }
            statementList.add(new WeakReference(statement));
        }
    }

    public EthConnection(String url, Properties props) throws SQLException {
        super();
        this.url = url;
        this.props = props;
        if (props.getProperty(DriverConstants.IPC) != null) {
            String path = props.getProperty(DriverConstants.IPC);
            if (props.getProperty(DriverConstants.IPC_OS) != null) {
                LOGGER.info("Connecting to ethereum with ipc file on windows location : " + path);
                web3jClient = Web3j.build(new WindowsIpcService(path));
            } else {
                LOGGER.info("Connecting to ethereum with ipc file on unix location : " + path);
                web3jClient = Web3j.build(new UnixIpcService(path));
            }
        } else if (props.containsKey(DriverConstants.INFURAURL)) {
            String httpsUrl = DriverConstants.HTTPPSREFIX + props.getProperty(DriverConstants.INFURAURL);
            web3jClient = Web3j.build(new HttpService(httpsUrl));

        } else {
            String httpUrl = DriverConstants.HTTPPREFIX + props.getProperty(DriverConstants.HOSTNAME)
                    + DriverConstants.COLON + props.getProperty(DriverConstants.PORTNUMBER);
            LOGGER.info("Connecting to ethereum with rpcurl : " + httpUrl);
            web3jClient = Web3j.build(new HttpService(httpUrl));
        }
        verifyConnection();
        LOGGER.info("Connected to ethereum ");
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        if (isClosed)
            throw new BlkchnException("No operations allowed after statement closed.");
        this.url = url;
    }

    public Properties getInfo() {
        return props;
    }

    public void setInfo(Properties info) {
        this.props = info;
    }

    @Override
    public boolean isWrapperFor(Class<?> arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T unwrap(Class<T> arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws SQLException {
        realClose();
    }

    private void realClose() throws SQLException {
        if (this.isClosed) {
            return;
        }
        try {
            this.url = null;
            this.props = null;
            this.web3jClient = null;
            this.isClosed = true;
            closeAllOpenStatements();
            this.statementList = new ArrayList();
        } catch (Exception e) {
            throw new BlkchnException("Error while closing connection", e);
        }
    }

    private void closeAllOpenStatements() throws SQLException {
        for (Object stm : statementList) {
            if (stm != null && ((WeakReference) stm).get() != null)
                ((EthStatement) ((WeakReference) stm).get()).close();
        }
    }

    @Override
    public void commit() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Statement createStatement() throws SQLException {
        LOGGER.info("Entering into Create Statement Block");
        return createStatement(java.sql.ResultSet.FETCH_FORWARD, java.sql.ResultSet.CONCUR_READ_ONLY);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        if (isClosed)
            throw new BlkchnException("No operations allowed after connection closed.");
        EthStatement eStatement = new EthStatement(this, resultSetType, resultSetConcurrency);
        addNewStatement(eStatement);
        LOGGER.info("Statement Created");
        return eStatement;
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return true;
    }

    @Override
    public String getCatalog() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getHoldability() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return new EthDatabaseMetadata();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSchema() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.isClosed;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return prepareStatement(sql, java.sql.ResultSet.FETCH_FORWARD, java.sql.ResultSet.CONCUR_READ_ONLY);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return new EthPreparedStatement(this, sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rollback() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException();
    }

    protected void verifyConnection() throws SQLException {
        LOGGER.info("verifying the connection. ");
        try {
            web3jClient.web3ClientVersion().send().getWeb3ClientVersion();
        } catch (Exception e) {
            LOGGER.error("Couldn't connect with ethereum. please check the rpcurl");
            throw new SQLException("Couldn't connect with ethereum. please validate the connection url");
        }
    }

}

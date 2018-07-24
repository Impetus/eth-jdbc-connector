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
package com.impetus.eth.test;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;

import com.impetus.blkch.BlkchnErrorListener;
import com.impetus.blkch.sql.BlkType;
import com.impetus.blkch.sql.generated.BlkchnSqlLexer;
import com.impetus.blkch.sql.generated.BlkchnSqlParser;
import com.impetus.blkch.sql.parser.*;
import com.impetus.eth.parser.EthPhysicalPlan;
import com.impetus.eth.parser.EthQueryExecutor;
import com.impetus.eth.query.EthColumns;
import junit.framework.TestCase;

import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.impetus.blkch.sql.DataFrame;
import com.impetus.eth.jdbc.EthResultSet;
import com.impetus.eth.jdbc.EthResultSetMetaData;
import com.impetus.test.catagory.UnitTest;

@Category(UnitTest.class)
public class TestEthResultSetMetadata extends TestCase {

    String queryT1 = "select * from block";
    String queryT2 = "select count(*), sum(blocknumber) as sum, blocknumber, transactions from block";
    Map<String,Integer> dataTypeMapQ1 = null;
    Map<String,Integer> dataTypeMapQ2 = null;
    ResultSet queryResultSetQ1 = null;
    ResultSet queryResultSetQ2 = null;
    String[] columns = { EthColumns.BLOCKNUMBER, EthColumns.HASH, EthColumns.PARENTHASH, EthColumns.NONCE,
            EthColumns.SHA3UNCLES, EthColumns.LOGSBLOOM, EthColumns.TRANSACTIONSROOT, EthColumns.STATEROOT,
            EthColumns.RECEIPTSROOT, EthColumns.AUTHOR, EthColumns.MINER, EthColumns.MIXHASH,
            EthColumns.TOTALDIFFICULTY, EthColumns.EXTRADATA, EthColumns.SIZE, EthColumns.GASLIMIT,
            EthColumns.GASUSED, EthColumns.TIMESTAMP, EthColumns.TRANSACTIONS, EthColumns.UNCLES,
            EthColumns.SEALFIELDS };

    String[] columnsQ2 = { "count(*)", "sum(blocknumber)", "blocknumber", "transactions" };

    int stringType = Types.VARCHAR;
    int bigintType = Types.BIGINT;
    int objectType = BlkType.JAVA_LIST_STRING;

    @Override
    protected void setUp() {
        LogicalPlan logicalPlanT1 = getLogicalPlan(queryT1);
        LogicalPlan logicalPlanT2 = getLogicalPlan(queryT2);
        EthQueryExecutor qext1 = new EthQueryExecutor(logicalPlanT1,null,null);
        EthQueryExecutor qext2 = new EthQueryExecutor(logicalPlanT2,null,null);
        dataTypeMapQ1 = qext1.computeDataTypeColumnMap();
        dataTypeMapQ2 = qext2.computeDataTypeColumnMap();
        PhysicalPlan physicalPlanT1 = new EthPhysicalPlan(logicalPlanT1);
        PhysicalPlan physicalPlanT2 = new EthPhysicalPlan(logicalPlanT2);

        List<List<Object>> data = new ArrayList<List<Object>>();
        DataFrame dfT1 = new DataFrame(data, columns, physicalPlanT1.getColumnAliasMapping());
        DataFrame dfT2 = new DataFrame(data, columnsQ2, physicalPlanT2.getColumnAliasMapping());

        queryResultSetQ1 = new EthResultSet(dfT1, java.sql.ResultSet.FETCH_FORWARD, java.sql.ResultSet.CONCUR_READ_ONLY,"block", dataTypeMapQ1);
        queryResultSetQ2 = new EthResultSet(dfT2, java.sql.ResultSet.FETCH_FORWARD, java.sql.ResultSet.CONCUR_READ_ONLY, "block", dataTypeMapQ2);
    }

    @Test
    public void testEthResultSetQ1Size() {
        int actual = dataTypeMapQ1.size();
        assertEquals(21,actual);
    }

    @Test
    public void testEthResultSetQ2Size() {
        int actual = dataTypeMapQ2.size();
        assertEquals(4,actual);
    }

    @Test
    public void testDataTypeMapQ2DataType() {
        assertEquals(bigintType,(int)dataTypeMapQ2.get(columnsQ2[0]));
        assertEquals(bigintType,(int)dataTypeMapQ2.get(columnsQ2[1]));
        assertEquals(bigintType,(int)dataTypeMapQ2.get(columnsQ2[2]));
        assertEquals(objectType,(int)dataTypeMapQ2.get(columnsQ2[3]));
    }

    @Test
    public void testEthResultSetQ2DataType() {
        int col1DT = Integer.MIN_VALUE;
        int col2DT = Integer.MIN_VALUE;
        int col3DT = Integer.MIN_VALUE;
        int col4DT = Integer.MIN_VALUE;
        try {
            ResultSetMetaData rsMetaData = queryResultSetQ2.getMetaData();
            List colList = Arrays.asList(columnsQ2);
            col1DT = rsMetaData.getColumnType(1);
            col2DT = rsMetaData.getColumnType(2);
            col3DT = rsMetaData.getColumnType(3);
            col4DT = rsMetaData.getColumnType(4);
        }catch (SQLException e) {

        }
        assertEquals(bigintType,col1DT);
        assertEquals(bigintType,col2DT);
        assertEquals(bigintType,col3DT);
        assertEquals(objectType,col4DT);
    }

    @Test
    public void testDataTypeMapQ1DataType() {
        assertEquals(bigintType,(int)dataTypeMapQ1.get(EthColumns.BLOCKNUMBER));
        assertEquals(stringType,(int)dataTypeMapQ1.get(EthColumns.EXTRADATA));
        assertEquals(bigintType,(int)dataTypeMapQ1.get(EthColumns.SIZE));
        assertEquals(bigintType,(int)dataTypeMapQ1.get(EthColumns.GASLIMIT));
        assertEquals(bigintType,(int)dataTypeMapQ1.get(EthColumns.GASUSED));
        assertEquals(bigintType,(int)dataTypeMapQ1.get(EthColumns.TIMESTAMP));
        assertEquals(objectType,(int)dataTypeMapQ1.get(EthColumns.TRANSACTIONS));
        assertEquals(objectType,(int)dataTypeMapQ1.get(EthColumns.SEALFIELDS));
        assertEquals(objectType,(int)dataTypeMapQ1.get(EthColumns.UNCLES));
        assertEquals(bigintType,(int)dataTypeMapQ1.get(EthColumns.TOTALDIFFICULTY));
        assertEquals(stringType,(int)dataTypeMapQ1.get(EthColumns.MIXHASH));
        assertEquals(stringType,(int)dataTypeMapQ1.get(EthColumns.MINER));
        assertEquals(stringType,(int)dataTypeMapQ1.get(EthColumns.AUTHOR));
        assertEquals(stringType,(int)dataTypeMapQ1.get(EthColumns.RECEIPTSROOT));
        assertEquals(stringType,(int)dataTypeMapQ1.get(EthColumns.STATEROOT));
        assertEquals(stringType,(int)dataTypeMapQ1.get(EthColumns.TRANSACTIONSROOT));
        assertEquals(stringType,(int)dataTypeMapQ1.get(EthColumns.LOGSBLOOM));
        assertEquals(stringType,(int)dataTypeMapQ1.get(EthColumns.SHA3UNCLES));
        assertEquals(stringType,(int)dataTypeMapQ1.get(EthColumns.NONCE));
        assertEquals(stringType,(int)dataTypeMapQ1.get(EthColumns.PARENTHASH));
        assertEquals(stringType,(int)dataTypeMapQ1.get(EthColumns.HASH));
    }

    @Test
    public void testEthResultSetQ1DataType() {
        int col1DT = Integer.MIN_VALUE;
        int col2DT = Integer.MIN_VALUE;
        int col3DT = Integer.MIN_VALUE;
        int col4DT = Integer.MIN_VALUE;
        int col5DT = Integer.MIN_VALUE;
        int col6DT = Integer.MIN_VALUE;
        int col7DT = Integer.MIN_VALUE;
        int col8DT = Integer.MIN_VALUE;
        int col9DT = Integer.MIN_VALUE;
        int col10DT = Integer.MIN_VALUE;
        int col11DT = Integer.MIN_VALUE;
        int col12DT = Integer.MIN_VALUE;
        int col13DT = Integer.MIN_VALUE;
        int col14DT = Integer.MIN_VALUE;
        int col15DT = Integer.MIN_VALUE;
        int col16DT = Integer.MIN_VALUE;
        int col17DT = Integer.MIN_VALUE;
        int col18DT = Integer.MIN_VALUE;
        int col19DT = Integer.MIN_VALUE;
        int col20DT = Integer.MIN_VALUE;
        int col21DT = Integer.MIN_VALUE;

        try {
            ResultSetMetaData rsMetaData = queryResultSetQ1.getMetaData();
            List colList = Arrays.asList(columns);
            col1DT = rsMetaData.getColumnType(colList.indexOf(EthColumns.BLOCKNUMBER)+1);
            col2DT = rsMetaData.getColumnType(colList.indexOf(EthColumns.EXTRADATA)+1);
            col3DT = rsMetaData.getColumnType(colList.indexOf(EthColumns.SIZE)+1);
            col4DT = rsMetaData.getColumnType(colList.indexOf(EthColumns.GASLIMIT)+1);
            col5DT = rsMetaData.getColumnType(colList.indexOf(EthColumns.GASUSED)+1);
            col6DT = rsMetaData.getColumnType(colList.indexOf(EthColumns.TIMESTAMP)+1);
            col7DT = rsMetaData.getColumnType(colList.indexOf(EthColumns.TRANSACTIONS)+1);
            col8DT = rsMetaData.getColumnType(colList.indexOf(EthColumns.SEALFIELDS)+1);
            col9DT = rsMetaData.getColumnType(colList.indexOf(EthColumns.UNCLES)+1);
            col10DT = rsMetaData.getColumnType(colList.indexOf(EthColumns.TOTALDIFFICULTY)+1);
            col11DT = rsMetaData.getColumnType(colList.indexOf(EthColumns.MIXHASH)+1);
            col12DT = rsMetaData.getColumnType(colList.indexOf(EthColumns.MINER)+1);
            col13DT = rsMetaData.getColumnType(colList.indexOf(EthColumns.AUTHOR)+1);
            col14DT = rsMetaData.getColumnType(colList.indexOf(EthColumns.RECEIPTSROOT)+1);
            col15DT = rsMetaData.getColumnType(colList.indexOf(EthColumns.STATEROOT)+1);
            col16DT = rsMetaData.getColumnType(colList.indexOf(EthColumns.TRANSACTIONSROOT)+1);
            col17DT = rsMetaData.getColumnType(colList.indexOf(EthColumns.LOGSBLOOM)+1);
            col18DT = rsMetaData.getColumnType(colList.indexOf(EthColumns.SHA3UNCLES)+1);
            col19DT = rsMetaData.getColumnType(colList.indexOf(EthColumns.NONCE)+1);
            col20DT = rsMetaData.getColumnType(colList.indexOf(EthColumns.PARENTHASH)+1);
            col21DT = rsMetaData.getColumnType(colList.indexOf(EthColumns.HASH)+1);
        } catch (SQLException e) {

        }
        assertEquals(bigintType,col1DT);
        assertEquals(stringType,col2DT);
        assertEquals(bigintType,col3DT);
        assertEquals(bigintType,col4DT);
        assertEquals(bigintType,col5DT);
        assertEquals(bigintType,col6DT);
        assertEquals(objectType,col7DT);
        assertEquals(objectType,col8DT);
        assertEquals(objectType,col9DT);
        assertEquals(bigintType,col10DT);
        assertEquals(stringType,col11DT);
        assertEquals(stringType,col12DT);
        assertEquals(stringType,col13DT);
        assertEquals(stringType,col14DT);
        assertEquals(stringType,col15DT);
        assertEquals(stringType,col16DT);
        assertEquals(stringType,col17DT);
        assertEquals(stringType,col18DT);
        assertEquals(stringType,col19DT);
        assertEquals(stringType,col20DT);
        assertEquals(stringType,col21DT);
    }

    public LogicalPlan getLogicalPlan(String sqlText) {
        LogicalPlan logicalPlan = null;
        BlkchnSqlParser parser = getParser(sqlText);

        parser.removeErrorListeners();
        parser.addErrorListener(BlkchnErrorListener.INSTANCE);

        AbstractSyntaxTreeVisitor astBuilder = new BlockchainVisitor();
        logicalPlan = (LogicalPlan) astBuilder.visitSingleStatement(parser.singleStatement());
        return logicalPlan;
    }

    public BlkchnSqlParser getParser(String sqlText) {
        BlkchnSqlLexer lexer = new BlkchnSqlLexer(new CaseInsensitiveCharStream(sqlText));

        lexer.removeErrorListeners();
        lexer.addErrorListener(BlkchnErrorListener.INSTANCE);

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        BlkchnSqlParser parser = new BlkchnSqlParser(tokens);
        return parser;
    }
}

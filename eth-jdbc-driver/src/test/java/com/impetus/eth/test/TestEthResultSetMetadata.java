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

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.impetus.blkch.BlkchnErrorListener;
import com.impetus.blkch.sql.generated.BlkchnSqlLexer;
import com.impetus.blkch.sql.generated.BlkchnSqlParser;
import com.impetus.blkch.sql.parser.AbstractSyntaxTreeVisitor;
import com.impetus.blkch.sql.parser.BlockchainVisitor;
import com.impetus.blkch.sql.parser.CaseInsensitiveCharStream;
import com.impetus.blkch.sql.parser.LogicalPlan;
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
    String queryT2 = "select count(*), sum(blocknumber), blocknumber, transactions from block";
    Map<String,Integer> dataTypeMapQ1 = null;
    Map<String,Integer> dataTypeMapQ2 = null;


    @Override
    protected void setUp() {
        LogicalPlan logicalPlanT1 = getLogicalPlan(queryT1);
        LogicalPlan logicalPlanT2 = getLogicalPlan(queryT2);
        EthQueryExecutor qext1 = new EthQueryExecutor(logicalPlanT1,null,null);
        EthQueryExecutor qext2 = new EthQueryExecutor(logicalPlanT2,null,null);
        dataTypeMapQ1 = qext1.computeDataTypeColumnMap();
        dataTypeMapQ2 = qext2.computeDataTypeColumnMap();
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
    public void testEthResultSetQ1DataType() {
        int stringType = Types.VARCHAR;
        int bigintType = Types.BIGINT;
        int objectType = Types.JAVA_OBJECT;
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
        assertEquals(bigintType,(int)dataTypeMapQ1.get(EthColumns.NONCE));
        assertEquals(stringType,(int)dataTypeMapQ1.get(EthColumns.PARENTHASH));
        assertEquals(stringType,(int)dataTypeMapQ1.get(EthColumns.HASH));
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

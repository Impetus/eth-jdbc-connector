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
package com.impetus.eth.jdbc.test;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.impetus.blkch.sql.DataFrame;
import com.impetus.blkch.sql.parser.LogicalPlan;
import com.impetus.blkch.sql.parser.PhysicalPlan;
import com.impetus.eth.jdbc.EthResultSet;
import com.impetus.eth.parser.EthPhysicalPlan;
import com.impetus.eth.parser.EthQueryExecutor;
import com.impetus.eth.test.util.CreateLogicalPlan;
import com.impetus.test.catagory.UnitTest;

import ch.qos.logback.core.subst.Token.Type;
import junit.framework.TestCase;

@Category(UnitTest.class)
public class TestEthResultSetMetadata extends TestCase {

    ResultSetMetaData rsmd = null;

    @Override
    protected void setUp() throws Exception {

        LogicalPlan logicalPlan = CreateLogicalPlan.getLogicalPlan(
                "select count(*) , sum(blocknumber) as sum, value from transaction where blocknumber >12345 and blocknumber< 12354");
        EthQueryExecutor qExecutor = new EthQueryExecutor(logicalPlan, null, null);
        Map<String, Integer> dataTypeMap = qExecutor.computeDataTypeColumnMap();
        PhysicalPlan physicalPlan = new EthPhysicalPlan(logicalPlan);

        List<List<Object>> data = new ArrayList<List<Object>>();
        String[] columns = { "count(*)", "sum(blocknumber)", "value" };
        DataFrame df = new DataFrame(data, columns, physicalPlan.getColumnAliasMapping());
        EthResultSet rs = new EthResultSet(df, java.sql.ResultSet.FETCH_FORWARD, java.sql.ResultSet.CONCUR_READ_ONLY,
                "block", dataTypeMap);
        rsmd = rs.getMetaData();

    }

    @Test
    public void testGetDataType() {
        int colType = 0;
        try {
            colType = rsmd.getColumnType(2);
        } catch (Exception e) {
            e.printStackTrace();
        }

        assertEquals(Types.DOUBLE, colType);
    }

    @Test
    public void testGetDataTypeError() {
        String message = "";
        try {
            rsmd.getColumnType(4);
        } catch (Exception e) {
            message = e.getMessage();
        }

        assertEquals("Column is not present in result set", message);
    }
    
    @Test
    public void testGetColumnTypeName() {
        String colType = "";
        try {
            colType = rsmd.getColumnTypeName(2);
        } catch (Exception e) {
            e.printStackTrace();
        }

        assertEquals("Double", colType);
    }
}

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
package com.impetus.eth.parser.test;

import java.sql.DriverManager;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.impetus.blkch.sql.parser.LogicalPlan;
import com.impetus.blkch.sql.query.DirectAPINode;
import com.impetus.blkch.sql.query.RangeNode;
import com.impetus.eth.jdbc.EthConnection;
import com.impetus.eth.parser.EthQueryExecutor;
import com.impetus.eth.test.util.ConnectionUtil;
import com.impetus.eth.test.util.CreateLogicalPlan;
import com.impetus.test.catagory.IntegrationTest;

import junit.framework.TestCase;

@Category(IntegrationTest.class)
public class TestEthQueryExecutorIntegration extends TestCase {
    private String url = ConnectionUtil.getEthUrl();

    private String driverClass = "com.impetus.eth.jdbc.EthDriver";

    EthQueryExecutor ethQueryExec = null;

    @Override
    protected void setUp() throws Exception {
        Class.forName(driverClass);
        EthConnection conn = (EthConnection) DriverManager.getConnection(url, null);
        LogicalPlan logicalPlan = CreateLogicalPlan.getLogicalPlan(
                "select blocknumber from transaction where blocknumber< 3800000  and blocknumber >3799990");
        ethQueryExec = new EthQueryExecutor(logicalPlan, conn.getWeb3jClient(), null);

    }

    @Test
    public void testGetFullRange() {
        boolean status = false;
        try {
            ethQueryExec.getFullRange();
            status = true;
        } catch (Exception e) {
        }
        assertEquals(true, status);
    }

    @Test
    public void testGetRangeNodeFromDataNode() {
        RangeNode rangeNode = null;
        try {
            DirectAPINode directAPINode = new DirectAPINode("transaction", "blocknumber", "3800000");
            rangeNode = ethQueryExec.processDirectAPINodeForRange(directAPINode);
        } catch (Exception e) {
        }
        assertEquals("[3800000-3800000]", rangeNode.getRangeList().getRanges().get(0).toString());
    }

    @Test
    public void testGetRangeNodeFromDataNodeBlockHash() {
        RangeNode rangeNode = null;
        try {
            DirectAPINode directAPINode = new DirectAPINode("transaction", "blockhash",
                    "0xdf353674484a0cc956e724aa7e7712ee8317dd771c0a74aedbe24727a63c204c");
            rangeNode = ethQueryExec.processDirectAPINodeForRange(directAPINode);

        } catch (Exception e) {
        }
        assertEquals("[3800000-3800000]", rangeNode.getRangeList().getRanges().get(0).toString());
    }

    
    @Test
    public void testGetRangeNodeFromDataNodeHash() {
        RangeNode rangeNode = null;
        try {
            DirectAPINode directAPINode = new DirectAPINode("transaction", "hash",
                    "0x0464f39e3ed543a3c4def4e23acedab975c00b8c4806eaae8ec1e5c4cd024e11");
            rangeNode = ethQueryExec.processDirectAPINodeForRange(directAPINode);

        } catch (Exception e) {
        }
        assertEquals("[3800000-3800000]", rangeNode.getRangeList().getRanges().get(0).toString());
    }

}

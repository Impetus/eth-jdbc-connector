package com.impetus.eth.integration.test;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;

import com.impetus.blkch.sql.parser.LogicalPlan;
import com.impetus.blkch.sql.query.DataNode;
import com.impetus.blkch.sql.query.RangeNode;
import com.impetus.blkch.util.LongRangeOperations;
import com.impetus.blkch.util.Range;
import com.impetus.eth.jdbc.EthConnection;
import com.impetus.eth.jdbc.EthStatement;
import com.impetus.eth.parser.EthQueryExecutor;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.eth.test.util.ConnectionUtil;
import com.impetus.test.catagory.IntegrationTest;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.methods.response.EthBlock;

@Category(IntegrationTest.class)
public class TestGetProbableRange {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestAlias.class);

    @Test
    public void testAlias() {

        String url = "jdbc:blkchn:ethereum://ropsten.infura.io/1234";
        String driverClass = "com.impetus.eth.jdbc.EthDriver";
        try{
            Class.forName(driverClass);
            EthConnection ethConnection = (EthConnection) DriverManager.getConnection(url, null);
            EthStatement stmt = (EthStatement) ethConnection.createStatement();
            String sql = "Select * from block";

            LOGGER.info("===============Test getProbableRange===============");
            RangeNode rangeNode1 = stmt.getProbableRange(sql);
            LogicalPlan logicalPlan = stmt.getLogicalPlan(sql);
            EthQueryExecutor executor = new EthQueryExecutor(logicalPlan, ethConnection.getWeb3jClient(), ethConnection.getInfo());
            RangeNode rangeNode2 = executor.getProbableRange();
            assert(rangeNode1.equals(rangeNode2));

            LOGGER.info("===============Test getRangeNodeFromDataNode===============");
            String blockNumber = "12";
            EthBlock block =
                    ethConnection.getWeb3jClient().ethGetBlockByNumber(DefaultBlockParameter.valueOf(new BigInteger(blockNumber)), true).send();
            DataNode dataNode= new DataNode<>("block", Arrays.asList(block.getBlock().getNumber().toString()));
            RangeNode rangeNode3 = executor.getRangeNodeFromDataNode(dataNode);
            RangeNode<BigInteger> rangeExpected = new RangeNode<>("block", "blocknumber");
            rangeExpected.getRangeList().addRange(new Range<>(new BigInteger(blockNumber), new BigInteger(blockNumber)));
            assert(rangeNode3.equals(rangeExpected));

            LOGGER.info("===============Test getRangeNodeFromDataNode For Wrong Value===============");
            DataNode dataNodeWrong = new DataNode<>("block", Arrays.asList());
            RangeNode rangeNodeWrong = executor.getRangeNodeFromDataNode(dataNodeWrong);
            RangeNode<BigInteger> rangeWrong = new RangeNode<>("block", "blocknumber");
            rangeWrong.getRangeList().addRange(new Range<>(new BigInteger("0"), new BigInteger("0")));
            assert(rangeNodeWrong.equals(rangeWrong));

            LOGGER.info("===============Test getFullRange===============");
            RangeNode rangeNode4 = executor.getFullRange();
            RangeNode<BigInteger> rangeExpected2 = new RangeNode<>("block", "blocknumber");
            rangeExpected2.getRangeList().addRange(new Range<>(new BigInteger("1"), (BigInteger)stmt.getBlockHeight()));
            assert(rangeNode4.equals(rangeExpected2));

            LOGGER.info("============Test Get DataNode For Wrong Values=========");


        }catch (Exception e){
            e.printStackTrace();
        }
    }
}

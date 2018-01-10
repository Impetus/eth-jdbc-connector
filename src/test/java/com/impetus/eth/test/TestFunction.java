package com.impetus.eth.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import junit.framework.TestCase;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.web3j.protocol.core.methods.response.EthBlock.Block;
import org.web3j.protocol.core.methods.response.Transaction;

import com.impetus.blkch.sql.parser.TreeNode;
import com.impetus.blkch.sql.query.Column;
import com.impetus.blkch.sql.query.FunctionNode;
import com.impetus.blkch.sql.query.IdentifierNode;
import com.impetus.eth.parser.Function;
import com.impetus.test.catagory.UnitTest;

@Category(UnitTest.class)
public class TestFunction extends TestCase {

    private List<Object> blockdata = new ArrayList<Object>();

    private List<Object> transactiondata = new ArrayList<Object>();

    private HashMap<String, Integer> columnNamesMap = new HashMap<>();

    private HashMap<String, Integer> transcolumnNamesMap = new HashMap<>();

    String table;

    protected void setUp() {

        columnNamesMap.put("value", 0);
        columnNamesMap.put("gas", 1);
        columnNamesMap.put("blocknumber", 2);

        for (int i = 0; i < 3; i++) {
            Block blockInfo = new Block();
            blockInfo.setNumber("0x1313");
            blockInfo.setGasUsed("0x131");
            blockInfo.setSize("0x131");
            blockdata.add(blockInfo);
        }

        transcolumnNamesMap.put("blocknumber", 0);
        transcolumnNamesMap.put("gas", 1);
        transcolumnNamesMap.put("value", 2);

        for (int i = 0; i < 3; i++) {
            Transaction transInfo = new Transaction();
            transInfo.setBlockNumber("0x1313");
            transInfo.setGas("0x131");
            transInfo.setValue("0x131");
            transactiondata.add(transInfo);
        }

    }

    @Test
    public void testBlockFunctionCount() {
        Function func = new Function(blockdata, columnNamesMap, table);
        Object count = func.computeFunction(getFunctionNode("count"));
        assertEquals(3, count);
    }

    @Test
    public void testBlockFunctionSum() {
        Function func = new Function(blockdata, columnNamesMap, table);
        Object sum = func.computeFunction(getFunctionNode("sum"));
        assertEquals(14649, Integer.parseInt(sum.toString()));
    }

    @Test
    public void testTransactionFunctionCount() {
        Function func = new Function(transactiondata, transcolumnNamesMap, "transactions");
        Object count = func.computeFunction(getFunctionNode("count"));
        assertEquals(3, count);
    }

    @Test
    public void testTransactionFunctionSum() {
        Function func = new Function(transactiondata, transcolumnNamesMap, "transactions");
        Object sum = func.computeFunction(getFunctionNode("sum"));
        assertEquals(14649, Integer.parseInt(sum.toString()));
    }

    @Test
    public void testUnidentifiedFunction() {
        Function func = new Function(transactiondata, transcolumnNamesMap, "transactions");
        String errorMessage = "";
        try {
            func.computeFunction(getFunctionNode("some_func"));
        } catch (Exception e) {
            errorMessage = e.getMessage();
        }
        assertEquals("Unidentified function: some_func", errorMessage);
    }

    @Test
    public void testCreateFunctionCol() {
        Function func = new Function();
        String funcName = func.createFunctionColName(getFunctionNode("sum"));
        assertEquals("sum(blocknumber)", funcName);
    }

    private FunctionNode getFunctionNode(String operation) {
        FunctionNode funcNode = new FunctionNode();
        TreeNode col = new Column();
        IdentifierNode colIn = new IdentifierNode("blocknumber");
        col.addChildNode(colIn);
        IdentifierNode funcIdent = new IdentifierNode(operation);
        funcNode.addChildNode(funcIdent);
        funcNode.addChildNode(col);
        return funcNode;
    }
}

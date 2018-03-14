package com.impetus.eth.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import junit.framework.TestCase;

import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.web3j.protocol.core.methods.response.Transaction;

import com.impetus.blkch.sql.generated.BlkchnSqlLexer;
import com.impetus.blkch.sql.generated.BlkchnSqlParser;
import com.impetus.blkch.sql.parser.AbstractSyntaxTreeVisitor;
import com.impetus.blkch.sql.parser.BlockchainVisitor;
import com.impetus.blkch.sql.parser.CaseInsensitiveCharStream;
import com.impetus.blkch.sql.parser.LogicalPlan;
import com.impetus.blkch.sql.query.SelectClause;
import com.impetus.blkch.sql.query.SelectItem;
import com.impetus.eth.jdbc.TransactionResultDataHandler;
import com.impetus.test.catagory.UnitTest;


@Category(UnitTest.class)
public class TestTransactionResultDataHandler extends TestCase {
    private List<Object> data = new ArrayList<Object>();

    private HashMap<String, Integer> columnNamesMap = new HashMap<>();

    protected void setUp() {
        columnNamesMap.put("value", 0);
        columnNamesMap.put("gas", 1);
        columnNamesMap.put("blocknumber", 2);

    }

    @Test
    public void testTransactionResult() {
        TransactionResultDataHandler trdh = new TransactionResultDataHandler();
        String query = "select blocknumber,value, gas from transactions";
        LogicalPlan logicalPlan = getLogicalPlan(query);
        SelectClause selectClause = logicalPlan.getQuery().getChildType(SelectClause.class, 0);
        List<SelectItem> selItems = selectClause.getChildType(SelectItem.class);
        Transaction transInfo = new Transaction();
        transInfo.setBlockNumber("0x1313");
        transInfo.setValue("0x131");
        transInfo.setGas("0x131");
        data.add(transInfo);
        List<List<Object>> result = trdh.convertToObjArray(data, selItems, null);
        assertEquals(4883, Integer.parseInt(result.get(0).get(0).toString()));
    }

    @Test
    public void testTransactionResultExtraSelect() {
        TransactionResultDataHandler trdh = new TransactionResultDataHandler();
        String query = "select blocknumber,value from transactions";
        LogicalPlan logicalPlan = getLogicalPlan(query);
        SelectClause selectClause = logicalPlan.getQuery().getChildType(SelectClause.class, 0);
        List<SelectItem> selItems = selectClause.getChildType(SelectItem.class);
        Transaction transInfo = new Transaction();
        transInfo.setBlockNumber("0x1313");
        transInfo.setValue("0x131");
        transInfo.setGas("0x131");
        data.add(transInfo);
        List<String> extraSelItems = new ArrayList<String>();
        extraSelItems.add("gas");
        List<List<Object>> result = trdh.convertToObjArray(data, selItems, extraSelItems);
        assertEquals(305, Integer.parseInt(result.get(0).get(2).toString()));
    }
    
    
    @Test
    public void testTransactionResultGroupBy() {
        TransactionResultDataHandler trdh = new TransactionResultDataHandler();
        String query = "select count(value),value from transactions group by value";
        LogicalPlan logicalPlan = getLogicalPlan(query);
        SelectClause selectClause = logicalPlan.getQuery().getChildType(SelectClause.class, 0);
        List<SelectItem> selItems = selectClause.getChildType(SelectItem.class);
        Transaction transInfo = new Transaction();
        transInfo.setBlockNumber("0x1313");
        transInfo.setValue("0x131");
        transInfo.setGas("0x131");
        data.add(transInfo);
        transInfo.setBlockNumber("0x1313");
        transInfo.setValue("0x131");
        transInfo.setGas("0x131");
        data.add(transInfo);
        List<String> groupByCols = new ArrayList<String>();
        groupByCols.add("gas");
        
        List<List<Object>> result = trdh.convertGroupedDataToObjArray(data, selItems, groupByCols);
        assertEquals(2, Integer.parseInt(result.get(0).get(0).toString()));
    }
    
    @Test
    public void testTransactionResultStar() {
        TransactionResultDataHandler trdh = new TransactionResultDataHandler();
        String query = "select * from transactions";
        LogicalPlan logicalPlan = getLogicalPlan(query);
        SelectClause selectClause = logicalPlan.getQuery().getChildType(SelectClause.class, 0);
        List<SelectItem> selItems = selectClause.getChildType(SelectItem.class);
        Transaction transInfo = new Transaction("0x1313", "0x1313", "0x1313", "0x1313", "0x1313", "0x1313", "0x1313", "0x1313", "0x1313", "0x1313"
                , "0x1313", "0x1313", "0x1313", "0x1313", "0x1313", "0x1313", 1);
       data.add(transInfo);
        
        List<List<Object>> result = trdh.convertToObjArray(data, selItems, null);
        assertEquals(1, Integer.parseInt(result.get(0).get(15).toString()));
    }

    public LogicalPlan getLogicalPlan(String sqlText) {
        LogicalPlan logicalPlan = null;
        BlkchnSqlParser parser = getParser(sqlText);
        AbstractSyntaxTreeVisitor astBuilder = new BlockchainVisitor();
        logicalPlan = (LogicalPlan) astBuilder.visitSingleStatement(parser.singleStatement());
        return logicalPlan;
    }

    public BlkchnSqlParser getParser(String sqlText) {
        BlkchnSqlLexer lexer = new BlkchnSqlLexer(new CaseInsensitiveCharStream(sqlText));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        BlkchnSqlParser parser = new BlkchnSqlParser(tokens);
        return parser;
    }
}

package com.impetus.eth.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import junit.framework.TestCase;

import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.web3j.protocol.core.methods.response.EthBlock.Block;

import com.impetus.blkch.sql.generated.BlkchnSqlLexer;
import com.impetus.blkch.sql.generated.BlkchnSqlParser;
import com.impetus.blkch.sql.parser.AbstractSyntaxTreeVisitor;
import com.impetus.blkch.sql.parser.BlockchainVisitor;
import com.impetus.blkch.sql.parser.CaseInsensitiveCharStream;
import com.impetus.blkch.sql.parser.LogicalPlan;
import com.impetus.blkch.sql.query.SelectClause;
import com.impetus.blkch.sql.query.SelectItem;
import com.impetus.eth.jdbc.BlockResultDataHandler;
import com.impetus.test.catagory.UnitTest;


@Category(UnitTest.class)
public class TestBlockResultDataHandler extends TestCase {
    private List<Object> data = new ArrayList<Object>();

    private HashMap<String, Integer> columnNamesMap = new HashMap<>();

    protected void setUp() {
        columnNamesMap.put("blocknumber", 0);
        columnNamesMap.put("gasused", 1);
        columnNamesMap.put("size", 2);
        for (int i = 0; i < 3; i++) {
            Block blockInfo = new Block();
            blockInfo.setNumber("0x1313");
            blockInfo.setGasUsed("0x131");
            blockInfo.setSize("0x131");
            blockInfo.setTotalDifficulty("0x131");
            blockInfo.setSize("0x131");
            blockInfo.setGasLimit("0x131");
            blockInfo.setGasUsed("0x131");
            blockInfo.setTimestamp("0x131");
            data.add(blockInfo);
        }

    }

    @Test
    public void testBlockResult() {
        BlockResultDataHandler brdh = new BlockResultDataHandler();
        String query = "select blocknumber,gasused, size from transactions";
        LogicalPlan logicalPlan = getLogicalPlan(query);
        SelectClause selectClause = logicalPlan.getQuery().getChildType(SelectClause.class, 0);
        List<SelectItem> selItems = selectClause.getChildType(SelectItem.class);

        List<List<Object>> result = brdh.convertToObjArray(data, selItems, null);
        assertEquals(4883, Integer.parseInt(result.get(0).get(0).toString()));
    }

    @Test
    public void testBlockResultExtraSelect() {
        BlockResultDataHandler brdh = new BlockResultDataHandler();
        String query = "select blocknumber,gasused from transactions";
        LogicalPlan logicalPlan = getLogicalPlan(query);
        SelectClause selectClause = logicalPlan.getQuery().getChildType(SelectClause.class, 0);
        List<SelectItem> selItems = selectClause.getChildType(SelectItem.class);
        List<String> extraSelItems = new ArrayList<String>();
        extraSelItems.add("size");
        List<List<Object>> result = brdh.convertToObjArray(data, selItems, extraSelItems);
        assertEquals(305, Integer.parseInt(result.get(0).get(2).toString()));
    }

    @Test
    public void testBlockResultGroupBy() {
        BlockResultDataHandler trdh = new BlockResultDataHandler();
        String query = "select count(size),size from transactions group by value";
        LogicalPlan logicalPlan = getLogicalPlan(query);
        SelectClause selectClause = logicalPlan.getQuery().getChildType(SelectClause.class, 0);
        List<SelectItem> selItems = selectClause.getChildType(SelectItem.class);
        List<String> groupByCols = new ArrayList<String>();
        groupByCols.add("blocknumber");

        List<List<Object>> result = trdh.convertGroupedDataToObjArray(data, selItems, groupByCols);
        assertEquals(3, Integer.parseInt(result.get(0).get(0).toString()));
    }

    @Test
    public void testBlockResultStar() {
        BlockResultDataHandler trdh = new BlockResultDataHandler();
        String query = "select * from transactions";
        LogicalPlan logicalPlan = getLogicalPlan(query);
        SelectClause selectClause = logicalPlan.getQuery().getChildType(SelectClause.class, 0);
        List<SelectItem> selItems = selectClause.getChildType(SelectItem.class);
        List<List<Object>> result = trdh.convertToObjArray(data, selItems, null);
        assertEquals(4883, Integer.parseInt(result.get(0).get(0).toString()));
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

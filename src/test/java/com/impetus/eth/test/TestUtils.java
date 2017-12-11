package com.impetus.eth.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.impetus.eth.parser.Utils;

import junit.framework.TestCase;

public class TestUtils extends TestCase {
    private List<String> selectColumns = new ArrayList<String>();

    private Map<String, String> aliasMapping = new HashMap<String, String>();

    @Override
    protected void setUp() throws Exception {
        selectColumns.add("blocknumber");
        selectColumns.add("gas");

        aliasMapping.put("gasp", "gasprice");
    }

    @Test
    public void testUtilsVerifyGroupedColumns() {
        List<String> groupedColumns = new ArrayList<String>();
        groupedColumns.add("gas");
        Utils.verifyGroupedColumns(selectColumns, groupedColumns, aliasMapping);
        assertTrue(true);
    }

    @Test
    public void testUtilsVerifyGroupedAlias() {
        List<String> groupedColumns = new ArrayList<String>();
        groupedColumns.add("gasp");
        Utils.verifyGroupedColumns(selectColumns, groupedColumns, aliasMapping);
        assertTrue(true);
    }

    @Test
    public void testUtilsVerifyGroupedColumnsNotTrue() {
        List<String> groupedColumns = new ArrayList<String>();
        groupedColumns.add("ga");
        String errorMsg = "";
        try {
            Utils.verifyGroupedColumns(selectColumns, groupedColumns, aliasMapping);
        } catch (Exception e) {
            errorMsg = e.getMessage();
        }
        assertEquals("Group by Column ga should exist in Select clause", errorMsg);
    }

    @Test
    public void testGetActaulGroupByCols() {
        List<String> groupByCols = new ArrayList<String>();
        groupByCols.add("gasp");
        groupByCols.add("blocknumber");
        List<String> actGrpByCols = Utils.getActualGroupByCols(groupByCols, aliasMapping);
        assertEquals("blocknumber", actGrpByCols.get(1));
    }
}

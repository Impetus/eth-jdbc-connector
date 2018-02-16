package com.impetus.eth.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.junit.Test;

import com.impetus.blkch.sql.DataFrame;
import com.impetus.blkch.sql.query.Column;
import com.impetus.blkch.sql.query.IdentifierNode;
import com.impetus.blkch.sql.query.LimitClause;
import com.impetus.blkch.sql.query.OrderItem;
import com.impetus.blkch.sql.query.OrderingDirection;
import com.impetus.blkch.sql.query.OrderingDirection.Direction;

public class TestOrderByClause extends TestCase {
    private List<List<Object>> data = new ArrayList<List<Object>>();

    private HashMap<String, Integer> columnNamesMap = new HashMap<>();

    private Map<String, String> aliasMapping = new HashMap<String, String>();

    String table;

    protected void setUp() {
        columnNamesMap.put("value", 0);
        columnNamesMap.put("gas", 1);
        columnNamesMap.put("blocknumber", 2);

        List<Object> returnRec = new ArrayList<Object>();
        returnRec.add(544444);
        returnRec.add(213234);
        returnRec.add(15675);
        data.add(returnRec);
        returnRec = new ArrayList<Object>();
        returnRec.add(544434);
        returnRec.add(213233);
        returnRec.add(156333);
        data.add(returnRec);
        returnRec = new ArrayList<Object>();
        returnRec.add(544434);
        returnRec.add(213233);
        returnRec.add(156333);
        data.add(returnRec);

        returnRec = new ArrayList<Object>();
        returnRec.add(544410);
        returnRec.add(213232);
        returnRec.add(156334);
        data.add(returnRec);
        returnRec = new ArrayList<Object>();
        returnRec.add(544411);
        returnRec.add(2132235);
        returnRec.add(1563324);
        data.add(returnRec);
        aliasMapping.put("val", "value");
    }

    @Test
    public void testOrderBy() {

        DataFrame df = new DataFrame(data, columnNamesMap, aliasMapping);
        List<OrderItem> orderitems = Arrays.asList(createOrderItem("val", Direction.ASC));
        df = df.order(orderitems);
        assertEquals(544444, df.getData().get(4).get(0));

    }

    @Test
    public void testOrderByWithAlias() {

        DataFrame df = new DataFrame(data, columnNamesMap, aliasMapping);
        List<OrderItem> orderitems = Arrays.asList(createOrderItem("val", Direction.ASC));
        df = df.order(orderitems);
        assertEquals(544444, df.getData().get(4).get(0));

    }

    @Test
    public void testOrderByExtraSelect() {

        DataFrame df = new DataFrame(data, columnNamesMap, aliasMapping);
        List<OrderItem> orderitems = Arrays.asList(createOrderItem("blocknumber", Direction.ASC));
        df = df.order(orderitems);
        assertEquals(544411, df.getData().get(4).get(0));

    }

    @Test
    public void testLimit() {
        DataFrame df = new DataFrame(data, columnNamesMap, aliasMapping);
        LimitClause limitClause = new LimitClause();
        IdentifierNode ifn = new IdentifierNode("2");
        limitClause.addChildNode(ifn);
        df = df.limit(limitClause);
        assertEquals(2, df.getData().size());
    }
    
    private static OrderItem createOrderItem(String colName, Direction direction) {
        OrderItem orderItem = new OrderItem();
        orderItem.addChildNode(new OrderingDirection(direction));
        Column column = createColumn(colName);
        orderItem.addChildNode(column);
        return orderItem;
    }
    
    private static Column createColumn(String colName) {
        Column column = new Column();
        IdentifierNode identifierNode = new IdentifierNode(colName);
        column.addChildNode(identifierNode);
        return column;
    }
}

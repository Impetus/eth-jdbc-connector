//package com.impetus.eth.test;
//
//import java.util.ArrayList;
//import java.util.List;
//
//import org.antlr.v4.runtime.CommonTokenStream;
//import org.junit.Test;
//import org.junit.experimental.categories.Category;
//
//import com.impetus.blkch.sql.generated.BlkchnSqlLexer;
//import com.impetus.blkch.sql.generated.BlkchnSqlParser;
//import com.impetus.blkch.sql.parser.AbstractSyntaxTreeVisitor;
//import com.impetus.blkch.sql.parser.BlockchainVisitor;
//import com.impetus.blkch.sql.parser.CaseInsensitiveCharStream;
//import com.impetus.blkch.sql.parser.LogicalPlan;
//import com.impetus.blkch.sql.query.Column;
//import com.impetus.blkch.sql.query.IdentifierNode;
//import com.impetus.blkch.sql.query.OrderItem;
//import com.impetus.blkch.sql.query.OrderingDirection;
//import com.impetus.blkch.sql.query.OrderingDirection.Direction;
//import com.impetus.eth.parser.APIConverter;
//import com.impetus.test.catagory.UnitTest;
//
//import junit.framework.TestCase;
//
//@Category(UnitTest.class)
//public class TestAPIConvertor extends TestCase {
//
//    @Override
//    protected void setUp() throws Exception {
//
//    }
//
//    @Test
//    public void testBaseAPIConvert() {
//        APIConverter apic = new APIConverter(
//                getLogicalPlan("select count(name) as count, name as name  from transactions"), null,null);
//        OrderItem orderItem = new OrderItem();
//        OrderingDirection orderDir = new OrderingDirection(Direction.ASC);
//        orderItem.addChildNode(orderDir);
//        Column column = new Column();
//        IdentifierNode colIdent = new IdentifierNode("name");
//        column.addChildNode(colIdent);
//        orderItem.addChildNode(column);
//        List<OrderItem> orderItems = new ArrayList<OrderItem>();
//        orderItems.add(orderItem);
//        apic.getorderList(orderItems);
//        assertTrue(true);
//    }
//
//    @Test
//    public void testExecuteQuery() {
//        boolean status = false;
//        try {
//            APIConverter apic = new APIConverter(
//                    getLogicalPlan("select count(name) as count, name as name  from transactions where blocknumber=123"),
//                    null,null);
//            apic.executeQuery();
//            status=true;
//        } catch (Exception e) {
//
//        }
//        assertFalse(status);
//    }
//
//    public LogicalPlan getLogicalPlan(String sqlText) {
//        LogicalPlan logicalPlan = null;
//        BlkchnSqlParser parser = getParser(sqlText);
//        AbstractSyntaxTreeVisitor astBuilder = new BlockchainVisitor();
//        logicalPlan = (LogicalPlan) astBuilder.visitSingleStatement(parser.singleStatement());
//        return logicalPlan;
//    }
//    public BlkchnSqlParser getParser(String sqlText) {
//        BlkchnSqlLexer lexer = new BlkchnSqlLexer(new CaseInsensitiveCharStream(sqlText));
//        CommonTokenStream tokens = new CommonTokenStream(lexer);
//        BlkchnSqlParser parser = new BlkchnSqlParser(tokens);
//        return parser;
//    }
//
//}

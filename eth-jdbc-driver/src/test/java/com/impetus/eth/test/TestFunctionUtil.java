package com.impetus.eth.test;

import com.impetus.blkch.sql.function.Parameters;
import com.impetus.blkch.sql.query.IdentifierNode;
import com.impetus.blkch.sql.query.ListAgrs;
import com.impetus.test.catagory.UnitTest;
import com.impetus.eth.parser.FunctionUtil;
import junit.framework.TestCase;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import static org.hamcrest.CoreMatchers.*;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertThat;

@Category(UnitTest.class)
public class TestFunctionUtil extends TestCase {

    @Test
    public void testHandleIdent() {
        IdentifierNode ident = new IdentifierNode("column",IdentifierNode.IdentType.STRING);
        List<Object> args = new ArrayList<>();
        List<Class> argsType = new ArrayList<>();
        FunctionUtil.handleIdent(ident,args,argsType);
        assertEquals(args.get(0).toString(),"column");
        assertEquals(argsType.get(0),String.class);
    }

    @Test
    public void testHandleList(){
        ListAgrs lstArgs = new ListAgrs();
        Parameters prms = new Parameters();
        lstArgs.addChildNode(prms);
        prms.addChildNode(new IdentifierNode("column1",IdentifierNode.IdentType.STRING));
        prms.addChildNode(new IdentifierNode("column2",IdentifierNode.IdentType.STRING));
        prms.addChildNode(new IdentifierNode("column3",IdentifierNode.IdentType.STRING));
        prms.addChildNode(new IdentifierNode("column4",IdentifierNode.IdentType.STRING));
        List<String> lstActual = new ArrayList<>();
        lstActual.add("column1");
        lstActual.add("column2");
        lstActual.add("column3");
        lstActual.add("column4");
        List<Object> args = new ArrayList<>();
        List<Class> argsType = new ArrayList<>();
        FunctionUtil.handleList(lstArgs,args,argsType);
        assertThat(args.get(0), is(lstActual));
        assertEquals(argsType.get(0),List.class);
    }

    @Test
    public void testHexStringToByteArray(){
        String actualString = "0x457468657275656d";
        byte[] returnString = FunctionUtil.hexStringToByteArray(actualString);
        assertEquals("Etheruem",new String(returnString));
    }
}

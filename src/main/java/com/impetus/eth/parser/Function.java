/******************************************************************************* 
 * * Copyright 2017 Impetus Infotech.
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
package com.impetus.eth.parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.web3j.protocol.core.methods.response.EthBlock.Block;
import org.web3j.protocol.core.methods.response.Transaction;

import com.impetus.blkch.sql.query.Column;
import com.impetus.blkch.sql.query.FunctionNode;
import com.impetus.blkch.sql.query.IdentifierNode;

/**
 * The Class Function.
 */
public class Function
{

    /** The rows. */
    public List rows;

    /** The column names map. */
    private HashMap<String, Integer> columnNamesMap;

    /** The table. */
    private String table;

    /**
     * Instantiates a new function.
     */
    public Function(){
        
    }
    /**
     * Instantiates a new function.
     *
     * @param rows
     *            the rows
     * @param columnNamesMap
     *            the column names map
     * @param table
     *            the table
     */
    public Function(List rows, HashMap<String, Integer> columnNamesMap, String table)
    {
        super();
        this.rows = rows;
        this.columnNamesMap = columnNamesMap;
        this.table = table;
    }

    /**
     * Compute function.
     *
     * @param function
     *            the function
     * @return the object
     */
    public Object computeFunction(FunctionNode function)
    {
        String func = function.getChildType(IdentifierNode.class, 0).getValue();
        List<Object> columnData = new ArrayList<>();
        if (function.hasChildType(FunctionNode.class))
        {
            columnData.add(computeFunction(function.getChildType(FunctionNode.class, 0)));
        }
        else
        {
            String colName = function.getChildType(Column.class, 0).getChildType(IdentifierNode.class, 0).getValue();
            if (!columnNamesMap.containsKey((colName.toLowerCase())))
            {
                throw new RuntimeException("Column " + colName + " doesn't exist in table");
            }
            for (Object recordInfo : rows)
            {
                if ("transactions".equalsIgnoreCase(table))
                    columnData.add(Utils.getTransactionColumnValue((Transaction) recordInfo, colName));
                else
                    columnData.add(Utils.getBlockColumnValue((Block) recordInfo, colName));

            }
        }
        switch (func)
        {
        case "count":
            return AggregationFunctions.count(columnData);
        case "sum":
            return AggregationFunctions.sum(columnData);
        default:
            throw new RuntimeException("Unidentified function: " + func);
        }
    }

    /**
     * Creates the function col name.
     *
     * @param function
     *            the function
     * @return the string
     */
    public String createFunctionColName(FunctionNode function)
    {
        String func = function.getChildType(IdentifierNode.class, 0).getValue();
        if (function.hasChildType(FunctionNode.class))
        {
            return func + "(" + createFunctionColName(function.getChildType(FunctionNode.class, 0)) + ")";
        }
        else
        {
            String colName = function.getChildType(Column.class, 0).getChildType(IdentifierNode.class, 0).getValue();
            return func + "(" + colName + ")";
        }
    }
}

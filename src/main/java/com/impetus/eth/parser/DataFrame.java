package com.impetus.eth.parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.impetus.blkch.sql.query.Column;
import com.impetus.blkch.sql.query.FunctionNode;
import com.impetus.blkch.sql.query.IdentifierNode;

public class DataFrame
{
    private String table;
    private List<String> columns;

    private Map<String, String> aliasMapping;
    
    private  HashMap<String, Integer> columnNamesMap;
    
    private List<List<Object>> data;

    DataFrame(List<List<Object>> data, HashMap<String, Integer> columnNamesMap, Map<String, String> aliasMapping,String table)
    {
        this.aliasMapping = aliasMapping;
        this.data = data;
        this.columnNamesMap=columnNamesMap;
        this.table=table;
    }

    public List<String> getColumns()
    {
        return columns;
    }

    public Map<String, String> getAliasMapping()
    {
        return aliasMapping;
    }

    public List<List<Object>> getData()
    {
        return data;
    }

  /*  public DataFrame select(List<SelectItem> cols)
    {
        List<List<Object>> returnData = new ArrayList<>();
        List<String> returnCols = new ArrayList<>();
        boolean columnsInitialized = false;
        for (List<Object> record : data)
        {
            List<Object> returnRec = new ArrayList<>();
            for (SelectItem col : cols)
            {
                if (col.hasChildType(StarNode.class))
                {
                    for (String colName : columns)
                    {
                        int colIndex = columns.indexOf(colName);
                        returnRec.add(record.get(colIndex));
                        if (!columnsInitialized)
                        {
                            returnCols.add(colName);
                        }
                    }
                }
                else if (col.hasChildType(Column.class))
                {
                    int colIndex;
                    String colName = col.getChildType(Column.class, 0).getChildType(IdentifierNode.class, 0).getValue();
                    if (columns.contains(colName))
                    {
                        colIndex = columns.indexOf(colName);
                        if (!columnsInitialized)
                        {
                            returnCols.add(colName);
                        }
                    }
                    else if (aliasMapping.containsKey(colName))
                    {
                        String actualCol = aliasMapping.get(colName);
                        colIndex = columns.indexOf(actualCol);
                        if (!columnsInitialized)
                        {
                            returnCols.add(actualCol);
                        }
                    }
                    else
                    {
                        throw new RuntimeException("Column " + colName + " doesn't exist in table");
                    }
                    returnRec.add(record.get(colIndex));
                }
                else if (col.hasChildType(FunctionNode.class))
                {
                    Object computeResult = computeFunction(col.getChildType(FunctionNode.class, 0));
                    returnRec.add(computeResult);
                    if (col.hasChildType(IdentifierNode.class))
                    {
                        if (!columnsInitialized)
                        {
                            returnCols.add(col.getChildType(IdentifierNode.class, 0).getValue());
                        }
                    }
                    else if (!columnsInitialized)
                    {
                        returnCols.add(createFunctionColName(col.getChildType(FunctionNode.class, 0)));
                    }
                }
            }
            returnData.add(returnRec);
            columnsInitialized = true;
        }
        System.out.println(returnCols);
        return new DataFrame(returnData, returnCols, aliasMapping);
    }
*/
    private Object computeFunction(FunctionNode function)
    {
        String func = function.getChildType(IdentifierNode.class, 0).getValue();
        List<Object> columnData = new ArrayList<>();
        if (function.hasChildType(FunctionNode.class))
        {
            columnData.add(computeFunction(function.getChildType(FunctionNode.class, 0)));
        }
        else
        {
            int colIndex;
            String colName = function.getChildType(Column.class, 0).getChildType(IdentifierNode.class, 0).getValue();
            if (columns.contains(colName))
            {
                colIndex = columns.indexOf(colName);
            }
            else if (aliasMapping.containsKey(colName))
            {
                String actualCol = aliasMapping.get(colName);
                colIndex = columns.indexOf(actualCol);
            }
            else
            {
                throw new RuntimeException("Column " + colName + " doesn't exist in table");
            }
            for (List<Object> record : data)
            {
                columnData.add(record.get(colIndex));
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

    private String createFunctionColName(FunctionNode function)
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

    public String getTable()
    {
        return table;
    }

    public HashMap<String, Integer> getColumnNamesMap()
    {
        return columnNamesMap;
    }

}

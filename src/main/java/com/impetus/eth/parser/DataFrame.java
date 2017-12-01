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

import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.impetus.blkch.sql.query.Column;
import com.impetus.blkch.sql.query.IdentifierNode;
import com.impetus.blkch.sql.query.LimitClause;
import com.impetus.blkch.sql.query.OrderItem;
import com.impetus.blkch.sql.query.OrderingDirection;

/**
 * The Class DataFrame.
 */
public class DataFrame
{

    /** The table. */
    private String table;

    /** The columns. */
    private List<String> columns;

    /** The alias mapping. */
    private Map<String, String> aliasMapping;

    /** The column names map. */
    private HashMap<String, Integer> columnNamesMap;

    /** The data. */
    private List<List<Object>> data;

    /**
     * Instantiates a new data frame.
     *
     * @param data
     *            the data
     * @param columnNamesMap
     *            the column names map
     * @param aliasMapping
     *            the alias mapping
     * @param table
     *            the table
     */
    public DataFrame(List<List<Object>> data, HashMap<String, Integer> columnNamesMap,
            Map<String, String> aliasMapping, String table)
    {
        this.aliasMapping = aliasMapping;
        this.data = data;
        this.columnNamesMap = columnNamesMap;
        this.table = table;
    }

    /**
     * Gets the columns.
     *
     * @return the columns
     */
    public List<String> getColumns()
    {
        return columns;
    }

    /**
     * Gets the alias mapping.
     *
     * @return the alias mapping
     */
    public Map<String, String> getAliasMapping()
    {
        return aliasMapping;
    }

    /**
     * Gets the data.
     *
     * @return the data
     */
    public List<List<Object>> getData()
    {
        return data;
    }

    /**
     * Gets the table.
     *
     * @return the table
     */
    public String getTable()
    {
        return table;
    }

    /**
     * Gets the column names map.
     *
     * @return the column names map
     */
    public HashMap<String, Integer> getColumnNamesMap()
    {
        return columnNamesMap;
    }

    public DataFrame limit(LimitClause limitClause)
    {
        String limitValue = limitClause.getChildType(IdentifierNode.class, 0).getValue();
        int limit;
        try
        {
            limit = Integer.parseInt(limitValue);
        }
        catch (NumberFormatException e)
        {
            throw new RuntimeException(e);
        }
        if (limit < 0)
        {
            throw new RuntimeException("limit value should not be less than zero");
        }
        List<List<Object>> limitedData = data.stream().limit(limit).collect(Collectors.toList());
        return new DataFrame(limitedData, columnNamesMap, aliasMapping, table);
    }

    public DataFrame order(Map<String, OrderingDirection> orderList, List<String> extraSelectCols)
    {
        Collections.sort(data, new Comparator<List<Object>>()
        {

            @Override
            public int compare(List<Object> first, List<Object> second)
            {
                for (Map.Entry<String, OrderingDirection> entry : orderList.entrySet())
                {
                    int colIndex;
                    if (!columnNamesMap.containsKey(entry.getKey()))
                    {
                      colIndex=columnNamesMap.get(aliasMapping.get(entry.getKey()));
                    }
                    else
                        colIndex = columnNamesMap.get(entry.getKey());

                    Object firstObject = first.get(colIndex);
                    Object secondObject = second.get(colIndex);
                    if (firstObject.equals(secondObject))
                    {
                        continue;
                    }
                    OrderingDirection direction = entry.getValue();
                    int diff;
                    if (firstObject instanceof Integer)
                    {
                        diff = (((Integer) firstObject) - ((Integer) secondObject)) < 0 ? -1 : +1;
                    }
                    else if (firstObject instanceof Long)
                    {
                        diff = (((Long) firstObject) - ((Long) secondObject)) < 0 ? -1 : +1;
                    }
                    else if (firstObject instanceof Double)
                    {
                        diff = (((Double) firstObject) - ((Double) secondObject)) < 0.0 ? -1 : +1;
                    }
                    else if (firstObject instanceof Date)
                    {
                        diff = (((Date) firstObject).getTime() - ((Date) secondObject).getTime()) < 0.0 ? -1 : +1;
                    }
                    else
                    {
                        diff = firstObject.toString().compareTo(secondObject.toString());
                    }
                    return direction.isAsc() ? diff : diff * -1;
                }
                return 0;
            }

        });
        if (!(null == extraSelectCols))
            for (int i = 0; i < data.size(); i++)
            {
                for (int j = 0; j < extraSelectCols.size(); j++)
                {
                    data.get(i).remove(data.get(i).size() - 1);
                    if (columnNamesMap.containsKey(extraSelectCols.get(j)))
                        columnNamesMap.remove(extraSelectCols.get(j));
                }
            }
        return new DataFrame(data, columnNamesMap, aliasMapping, table);
    }
}

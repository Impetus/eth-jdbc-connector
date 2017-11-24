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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    public DataFrame(List<List<Object>> data, HashMap<String, Integer> columnNamesMap, Map<String, String> aliasMapping,
            String table)
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

}

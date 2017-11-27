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
package com.impetus.eth.jdbc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.impetus.blkch.sql.query.SelectItem;

/**
 * The Interface DataHandler.
 * 
 * @author karthikp.manchala
 * 
 */
public interface DataHandler
{

    /**
     * Convert to obj array.
     *
     * @param rows            the rows
     * @param selItems the sel items
     * @return the array list
     */
    public ArrayList<List<Object>> convertToObjArray(List rows, List<SelectItem> selItems);

    /**
     * Gets the table name.
     *
     * @return the table name
     */
    public String getTableName();
    
    /**
     * Gets the column names map.
     *
     * @return the column names map
     */
    public  HashMap<String, Integer> getColumnNamesMap();
    
    /**
     * Convert grouped data to obj array.
     *
     * @param rows the rows
     * @param selItems the sel items
     * @param groupByCols the group by cols
     * @return the array list
     */
    public ArrayList<List<Object>> convertGroupedDataToObjArray(List rows, List<SelectItem> selItems, List<String> groupByCols);

}

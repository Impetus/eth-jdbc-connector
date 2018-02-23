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
import java.util.List;
import java.util.Map;

import com.impetus.blkch.sql.query.SelectItem;

/**
 * The Interface DataHandler.
 * 
 * @author karthikp.manchala
 * 
 */
public interface DataHandler {

    public ArrayList<List<Object>> convertToObjArray(List rows, List<SelectItem> selItems, List<String> extraSelectCols);

    public String getTableName();

    public Map<String, Integer> getColumnNamesMap();

    public ArrayList<List<Object>> convertGroupedDataToObjArray(List rows, List<SelectItem> selItems,
            List<String> groupByCols);

}

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

import java.util.List;

public class AggregationFunctions {

    public static int count(List<Object> column) {
        return column.size();
    }

    public static Object sum(List<Object> column) {
        if (column.size() == 0) {
            return 0;
        }
        if (column.get(0) instanceof Integer) {
            int sum = 0;
            for (Object cell : column) {
                sum += Integer.parseInt(cell.toString().trim());
            }
            return sum;
        } else if (column.get(0) instanceof Long) {
            long sum = 0;
            for (Object cell : column) {
                sum += Long.parseLong(cell.toString().trim());
            }
            return sum;
        } else {
            double sum = 0.0;
            for (Object cell : column) {
                sum += Double.parseDouble(cell.toString().trim());
            }
            return sum;
        }
    }

}

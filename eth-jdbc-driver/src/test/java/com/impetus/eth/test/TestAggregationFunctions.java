/******************************************************************************* 
* * Copyright 2018 Impetus Infotech.
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
package com.impetus.eth.test;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.impetus.blkch.sql.AggregationFunctions;
import com.impetus.test.catagory.UnitTest;

@Category(UnitTest.class)
public class TestAggregationFunctions extends TestCase {
    private List<Object> data = new ArrayList<Object>();

    @Override
    protected void setUp() {
        data.add(1234);
        data.add(12);
        data.add(1);
        data.add(232);
        data.add(433);
        data.add(242);
        data.add(234);
    }

    @Test
    public void testSumFunc() {

        Object sum = AggregationFunctions.sum(data);
        assertEquals(2388, sum);
    }
    
    @Test
    public void testCountFunc() {

        Object count = AggregationFunctions.count(data);
        assertEquals(7, count);
    }
}

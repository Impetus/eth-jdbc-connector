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
package com.impetus.eth.test;

import java.util.Properties;

import org.junit.Test;
import com.impetus.eth.jdbc.DriverConstants;
import com.impetus.eth.jdbc.EthDriver;

import junit.framework.TestCase;

/**
 * The Class TestEthDriver.
 * 
 * @author ashishk.shukla
 * 
 */
public class TestEthDriver extends TestCase
{
    
    
    @Test
    public void testMajorVersion()
    {
        EthDriver dc = new EthDriver();
        int x=dc.getMajorVersion();
        assertEquals(1, x);
    }
    
    
    @Test
    public void testPropMap()
    {
       String url = "jdbc:blkchn:ethereum://Host:9";
        EthDriver dc = new EthDriver();
        Properties prop=dc.getPropMap(url);
        assertEquals("Host", prop.get(DriverConstants.HOSTNAME));
    }
}

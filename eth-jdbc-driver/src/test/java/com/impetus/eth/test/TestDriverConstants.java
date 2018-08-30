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

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.impetus.eth.jdbc.DriverConstants;
import com.impetus.test.catagory.UnitTest;

import junit.framework.TestCase;

/**
 * The Class TestColumnsMap.
 * 
 * @author ashishk.shukla
 * 
 */
@Category(UnitTest.class)
public class TestDriverConstants extends TestCase
{

    
    protected void setUp()
    {

    }

    
    @Test
    public void testConstants()
    {
        assertEquals(":", DriverConstants.COLON);
        assertEquals("HOSTNAME", DriverConstants.HOSTNAME);
        assertEquals("https://", DriverConstants.HTTPPSREFIX);
        assertEquals("http://", DriverConstants.HTTPPREFIX);
        assertEquals("PORTNUMBER", DriverConstants.PORTNUMBER);
        assertEquals("INFURAURL", DriverConstants.INFURAURL);
        assertEquals("IPC", DriverConstants.IPC);
        assertEquals("IPC_OS", DriverConstants.IPC_OS);
        assertEquals("KEYSTORE_PATH", DriverConstants.KEYSTORE_PATH);
        assertEquals("KEYSTORE_PATH", DriverConstants.KEYSTORE_PATH);
        assertEquals(1,DriverConstants.MAJOR_VERSION);
        assertEquals(1, DriverConstants.MINOR_VERSION);
        assertEquals("jdbc:blkchn:", DriverConstants.DRIVERPREFIX);

    }

}

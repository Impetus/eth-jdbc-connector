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
package com.impetus.eth.test.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.impetus.blkch.BlkchnException;

public class ConnectionUtil
{
    private static Properties prop = new Properties();

    static
    {
        InputStream input = null;
        try
        {
            input = new FileInputStream("src/test/resources/connection.properties");
            prop.load(input);
            if (prop.getProperty("eth_url") == null)
            {
                throw new BlkchnException("eth_url not set in connection.properties");
            }

        }
        catch (IOException ex)
        {
            throw new BlkchnException(ex);
        }
        finally
        {
            if (input != null)
            {
                try
                {
                    input.close();
                }
                catch (IOException e)
                {
                    throw new BlkchnException(e);
                }
            }
        }
    }

    public static String getEthUrl()
    {
        return prop.getProperty("eth_url");
    }

    public static int getTimeout()
    {
        if(prop.getProperty("timeout") != null)
           return Integer.parseInt(prop.getProperty("timeout"));
        else
            return 120;
    }
    public static String getKeyStorePath(){
        File file = new File("src/test/resources/UTC--2017-09-11T04-53-29.614189140Z--8144c67b144a408abc989728e32965edf37adaa1");
        return file.getPath();
    }
    public static String getKeyStorePassword(){
        return "impetus123";
    }
}

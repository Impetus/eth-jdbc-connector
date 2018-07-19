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

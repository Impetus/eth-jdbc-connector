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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import com.impetus.blkch.jdbc.BlkchnDriver;

/**
 * The Class EthDriver.
 * 
 * @author ashishk.shukla
 * 
 */
public class EthDriver implements BlkchnDriver
{

    static
    {
        try
        {
            DriverManager.registerDriver(new EthDriver());
        }
        catch (SQLException e)
        {

        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Driver#acceptsURL(java.lang.String)
     */
    @Override
    public boolean acceptsURL(String url) throws SQLException
    {

        if (url == null)
        {
            return false;
        }
        return url.toLowerCase().startsWith(DriverConstants.DRIVERPREFIX);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Driver#connect(java.lang.String, java.util.Properties)
     */
    @Override
    public Connection connect(String url, Properties info) throws SQLException
    {

        if (url == null || !url.toLowerCase().startsWith(DriverConstants.DRIVERPREFIX))
        {
            return null;
        }
        Properties props = getPropMap(url);
        try
        {
            return new EthConnection(url, props);
        }
        catch (Exception e)
        {
            return null;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Driver#getMajorVersion()
     */
    @Override
    public int getMajorVersion()
    {

        return DriverConstants.MAJOR_VERSION;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Driver#getMinorVersion()
     */
    @Override
    public int getMinorVersion()
    {

        return DriverConstants.MINOR_VERSION;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Driver#getParentLogger()
     */
    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Driver#getPropertyInfo(java.lang.String,
     * java.util.Properties)
     */
    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
    {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Driver#jdbcCompliant()
     */
    @Override
    public boolean jdbcCompliant()
    {
        return false;
    }

    /**
     * Gets the prop map.
     *
     * @param url
     *            the url
     * @return the prop map
     */
    public static Properties getPropMap(String url)
    {

        Properties props = new Properties();

        StringBuilder token = new StringBuilder();
        int index = 0;

        index = nextColonIndex(url, index, token);
        if (!"jdbc".equalsIgnoreCase(token.toString()))
        {
            return null;
        }

        index = nextColonIndex(url, index, token);
        if (!"blkchn".equalsIgnoreCase(token.toString()))
        {
            return null;
        }

        index = nextColonIndex(url, index, token);
        if (!"ethereum".equalsIgnoreCase(token.toString()))
        {
            return null;
        }
        // connecting with ipc file
        if (url.contains(".ipc"))
        {
            String path = url.substring(23);
            props.setProperty(DriverConstants.IPC, path);
            if (path.indexOf(DriverConstants.COLON) > 0)
            {
                props.setProperty(DriverConstants.IPC_OS, "windows");
            }
            
            return props;
        }

        index = nextColonIndex(url, index, token);
        index = nextColonIndex(url, index, token);
        String hostName = token.toString();
        props.setProperty(DriverConstants.HOSTNAME, hostName);

        index = nextColonIndex(url, index, token);

        try
        {
            int port = Integer.parseInt(token.toString());
            props.setProperty(DriverConstants.PORTNUMBER, Integer.toString(port));
        }
        catch (NumberFormatException e)
        {
            return null;
        }

        return props;

    }

    /**
     * Next colon index.
     *
     * @param url
     *            the url
     * @param position
     *            the position
     * @param token
     *            the token
     * @return the int
     */
    private static int nextColonIndex(String url, int position, StringBuilder token)
    {
        token.setLength(0);
        while (position < url.length())
        {
            char chAtPos = url.charAt(position++);
            if (chAtPos == ':' || chAtPos == '@')
            {
                break;
            }

            if (chAtPos == '/')
            {
                if (position < url.length() && url.charAt(position) == '/')
                {
                    position++;
                }

                break;
            }

            token.append(chAtPos);
        }

        return position;
    }

}

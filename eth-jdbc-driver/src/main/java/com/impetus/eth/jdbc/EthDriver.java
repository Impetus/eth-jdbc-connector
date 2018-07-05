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
public class EthDriver implements BlkchnDriver {

    static {
        try {
            DriverManager.registerDriver(new EthDriver());
        } catch (SQLException e) {

        }
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {

        if (url == null) {
            return false;
        }
        return url.toLowerCase().startsWith(DriverConstants.DRIVERPREFIX);
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {

        if (url == null || !url.toLowerCase().startsWith(DriverConstants.DRIVERPREFIX)) {
            return null;
        }

        Properties props = getPropMap(url);
        if (info != null && props != null) {
            props.putAll(info);
        }
        return new EthConnection(url, props);
    }

    @Override
    public int getMajorVersion() {

        return DriverConstants.MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion() {

        return DriverConstants.MINOR_VERSION;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    public static Properties getPropMap(String url) {

        Properties props = new Properties();

        StringBuilder token = new StringBuilder();
        int index = 0;

        index = nextColonIndex(url, index, token);
        if (!"jdbc".equalsIgnoreCase(token.toString())) {
            return null;
        }

        index = nextColonIndex(url, index, token);
        if (!"blkchn".equalsIgnoreCase(token.toString())) {
            return null;
        }

        index = nextColonIndex(url, index, token);
        if (!"ethereum".equalsIgnoreCase(token.toString())) {
            return null;
        }
        if (url.contains(".ipc")) {
            String path = url.substring(23);
            props.setProperty(DriverConstants.IPC, path);
            if (path.indexOf(DriverConstants.COLON) > 0) {
                props.setProperty(DriverConstants.IPC_OS, "windows");
            }

            return props;
        } else if (url.contains("infura")) {
            String infuraUrl = url.substring(23);
            props.setProperty(DriverConstants.INFURAURL, infuraUrl);
            return props;
        }

        index = nextColonIndex(url, index, token);
        index = nextColonIndex(url, index, token);
        String hostName = token.toString();
        props.setProperty(DriverConstants.HOSTNAME, hostName);

        index = nextColonIndex(url, index, token);

        try {
            int port = Integer.parseInt(token.toString());
            props.setProperty(DriverConstants.PORTNUMBER, Integer.toString(port));
        } catch (NumberFormatException e) {
            return null;
        }

        return props;

    }

    private static int nextColonIndex(String url, int position, StringBuilder token) {
        token.setLength(0);
        while (position < url.length()) {
            char chAtPos = url.charAt(position++);
            if (chAtPos == ':' || chAtPos == '@') {
                break;
            }

            if (chAtPos == '/') {
                if (position < url.length() && url.charAt(position) == '/') {
                    position++;
                }

                break;
            }

            token.append(chAtPos);
        }

        return position;
    }

}

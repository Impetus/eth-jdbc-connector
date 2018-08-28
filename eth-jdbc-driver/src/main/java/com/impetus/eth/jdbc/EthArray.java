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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.blkch.BlkchnException;
import com.impetus.blkch.jdbc.BlkchnArray;

public class EthArray implements BlkchnArray {

    private static final Logger LOGGER = LoggerFactory.getLogger(EthResultSet.class);

    protected List<Object> arrayData;

    protected int baseType;

    public EthArray(List<Object> arrayData, int baseType) {
        this.arrayData = arrayData;
        this.baseType = baseType;
    }

    @Override
    public String getBaseTypeName() throws SQLException {
        throw new BlkchnException("Method not supported");
    }

    @Override
    public int getBaseType() throws SQLException {
        return baseType;
    }

    @Override
    public Object getArray() throws SQLException {
        if (baseType == java.sql.Types.JAVA_OBJECT || baseType == java.sql.Types.VARCHAR) {
            String[] arrayObj = new String[arrayData.size()];
            for (int i = 0; i < arrayData.size(); i++) {
                arrayObj[i] = arrayData.get(i).toString();
            }
            return arrayObj;
        } else {
            LOGGER.error("Array of type " + baseType + " not supported");
            throw new BlkchnException("Array of type " + baseType + " not supported");
        }
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        throw new BlkchnException("Method not supported");
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
        throw new BlkchnException("Method not supported");
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        throw new BlkchnException("Method not supported");
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        throw new BlkchnException("Method not supported");
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        throw new BlkchnException("Method not supported");
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        throw new BlkchnException("Method not supported");
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        throw new BlkchnException("Method not supported");
    }

    @Override
    public void free() throws SQLException {
        if (arrayData != null) {
            arrayData = null;
            baseType = 0;
        } else {
            LOGGER.info("Nothing to do || Resources are already released ");
        }
    }

}

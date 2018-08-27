package com.impetus.eth.jdbc;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class EthArray implements Array {
    protected List<Object> arrayData;

    public EthArray(List<Object> arrayData) {
        this.arrayData = arrayData;
    }

    @Override
    public String getBaseTypeName() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getBaseType() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object getArray() throws SQLException {
       System.out.println("Array Data Class "+arrayData.getClass());
        String[]  arrayObj= new String[arrayData.size()];
        for(int i=0;i<arrayData.size();i++) {
            arrayObj[i]=arrayData.get(i).toString();
        }
        return Collections.emptyList();
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void free() throws SQLException {
        // TODO Auto-generated method stub

    }

}

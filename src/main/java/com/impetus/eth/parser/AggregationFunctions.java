package com.impetus.eth.parser;

import java.util.List;

public class AggregationFunctions
{

    public static int count(List<Object> column)
    {
        return column.size();
    }

    public static Object sum(List<Object> column)
    {
        if (column.size() == 0)
        {
            return 0;
        }
        if (column.get(0) instanceof Integer)
        {
            int sum = 0;
            for (Object cell : column)
            {
                sum += Integer.parseInt(cell.toString().trim());
            }
            return sum;
        }
        else if (column.get(0) instanceof Long)
        {
            long sum = 0;
            for (Object cell : column)
            {
                sum += Long.parseLong(cell.toString().trim());
            }
            return sum;
        }
        else
        {
            double sum = 0.0;
            for (Object cell : column)
            {
                sum += Double.parseDouble(cell.toString().trim());
            }
            return sum;
        }
    }

}

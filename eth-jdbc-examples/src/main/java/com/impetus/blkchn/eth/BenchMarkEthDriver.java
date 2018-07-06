package com.impetus.blkchn.eth;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class BenchMarkEthDriver {

    static final int min = 1000;
    static final int max = 3000000;
    static final int[] blockSize = {1, 100, 200, 500, 1000, 3000, 20000};
    static final int numIter = 3;
    static Connection conn = null;
    static Random randomNum = null;

    public static void main(String args[]) throws Exception {
        String url = "jdbc:blkchn:ethereum://172.25.41.52:8545";
        String driverClass = "com.impetus.eth.jdbc.EthDriver";

        Class.forName(driverClass);
        conn = DriverManager.getConnection(url, null);
        randomNum = new Random();

        for (int ittr = 0; ittr < numIter; ittr++) {
            List<HashMap> queryList = getQueryList();
            for (Object qmap : queryList) {
                for (Object qtype : ((Map) qmap).keySet()) {
                    System.out.println(qtype + " for iteration " + (ittr + 1));
                    //int i = 0;
                    for (Object query : (List) ((HashMap) qmap).get((String) qtype)) {
                        //System.out.println(blockSize[i++]+"\t\t"+query);
                        runQuery((String) query);
                    }
                    System.out.println("\n\n");
                }
            }
        }
    }

    public static List<HashMap> getQueryList() {
        List queryList = new ArrayList<HashMap>();
        Map queryMap = new HashMap<String, ArrayList>();
        String q1 = "select * from block where blocknumber = " + getRandomNumber();//1
        queryMap.put("Simple Query", new ArrayList<String>());
        ((List) queryMap.get("Simple Query")).add(q1);
        for (int i : blockSize) {
            if (i == 1) continue;
            blockRange blnum = getRandomRange(i);
            ((List) queryMap.get("Simple Query")).add("select * from block where blocknumber > " + blnum.rmin + " and blocknumber < " + blnum.rmax);
        }

        String qO1 = "select * from block where blocknumber = " + getRandomNumber() + "  order by hash";//1
        queryMap.put("Order By Query", new ArrayList<String>());
        ((List) queryMap.get("Order By Query")).add(qO1);
        for (int i : blockSize) {
            if (i == 1) continue;
            blockRange blnum = getRandomRange(i);
            ((List) queryMap.get("Order By Query")).add("select * from block where blocknumber > " + blnum.rmin + " and blocknumber < " + blnum.rmax + " order by hash");
        }

        String qG1 = "select count(*) as cnt, blocknumber  from transaction where blocknumber = " + getRandomNumber() + "  group by blocknumber";//1
        queryMap.put("Group By Query", new ArrayList<String>());
        ((List) queryMap.get("Group By Query")).add(qG1);
        for (int i : blockSize) {
            if (i == 1) continue;
            blockRange blnum = getRandomRange(i);
            ((List) queryMap.get("Group By Query")).add("select count(*) as cnt, blocknumber from transaction where " +
                    "blocknumber > " + blnum.rmin + " and blocknumber < " + blnum.rmax + " group by blocknumber");
        }
        queryList.add(queryMap);
        return queryList;
    }

    public static blockRange getRandomRange(int n) {
        int rmin = getRandomNumber();
        return (new blockRange(rmin, rmin + n));
    }

    public static int getRandomNumber() {
        return min + randomNum.nextInt(max);
    }

    public static void runQuery(String q) {
        try {
            Statement stmt = conn.createStatement();
            Long start = System.currentTimeMillis();
            ResultSet rs1 = stmt.executeQuery(q);
            Long end = System.currentTimeMillis();
            while (rs1.next()) {
                //System.out.println(rs1.toString());
                break;
            }
            Long qt = (end - start);
            //System.out.println("\t\tStart Time :: "+start+" end Time :: "+end+" ,duration "+(end - start));
            //System.out.println("============================================================================\n\n");
            System.out.println(qt);
            stmt.close();
            //System.out.println("Statement is close "+stmt.isClosed());
            //System.out.println("Result is close "+rs1.isClosed());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class blockRange {
    public int rmin;
    public int rmax;

    public blockRange(int rmin, int rmax) {
        this.rmin = rmin;
        this.rmax = rmax;
    }
}

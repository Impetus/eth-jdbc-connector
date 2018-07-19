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
package com.impetus.eth.integration.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.blkch.BlkchnException;
import com.impetus.eth.jdbc.DriverConstants;
import com.impetus.eth.test.util.ConnectionUtil;
import com.impetus.test.catagory.IntegrationTest;

@Category(IntegrationTest.class)
public class TestInsertPreparedStatement {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestAlias.class);

    @Test
    public void testInsertQuery() {

        String url = ConnectionUtil.getEthUrl();
        String driverClass = "com.impetus.eth.jdbc.EthDriver";
        try {
            Class.forName(driverClass);
            Properties prop = new Properties();
            prop.put(DriverConstants.KEYSTORE_PATH,
                    "D:\\Ethereum\\Ashish\\UTC--2017-09-11T04-49-35.811622052Z--a76cd046cf6089fe2adcf1680fcede500e44bacd");
            prop.put(DriverConstants.KEYSTORE_PASSWORD, "impetus123");
            Connection conn = DriverManager.getConnection(url, prop);
            String query = "insert into transaction (toAddress, value, unit, async) values (?, ?, 'ether', true)";

            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setObject(2, 0.0001);
            stmt.setObject(1, "0x8144c67b144A408ABC989728e32965EDf37Adaa1");
            int i = stmt.executeUpdate();

            stmt.setObject(2, 0.0002);
            stmt.setObject(1, "0x8144c67b144A408ABC989728e32965EDf37Adaa1");
            stmt.executeUpdate();

            LOGGER.info(" status " + i);
            LOGGER.info("Connection closed " + conn.isClosed());
            LOGGER.info("Statement closed " + stmt.isClosed());

        } catch (Exception e) {
            throw new BlkchnException(e.getMessage());
        }

    }

}

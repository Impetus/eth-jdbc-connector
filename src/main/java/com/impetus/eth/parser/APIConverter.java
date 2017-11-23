package com.impetus.eth.parser;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.antlr.v4.parse.ANTLRParser.throwsSpec_return;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.core.methods.response.Transaction;
import org.web3j.protocol.core.methods.response.EthBlock.Block;
import org.web3j.protocol.core.methods.response.EthBlock.TransactionObject;
import org.web3j.protocol.core.methods.response.EthBlock.TransactionResult;

import com.impetus.blkch.sql.parser.LogicalPlan;
import com.impetus.blkch.sql.query.Column;
import com.impetus.blkch.sql.query.FilterItem;
import com.impetus.blkch.sql.query.FromItem;
import com.impetus.blkch.sql.query.IdentifierNode;
import com.impetus.blkch.sql.query.LogicalOperation;
import com.impetus.blkch.sql.query.SelectClause;
import com.impetus.blkch.sql.query.SelectItem;
import com.impetus.blkch.sql.query.Table;
import com.impetus.blkch.sql.query.WhereClause;
import com.impetus.eth.jdbc.BlockResultDataHandler;
import com.impetus.eth.jdbc.DataHandler;
import com.impetus.eth.jdbc.EthStatement;
import com.impetus.eth.jdbc.TransactionResultDataHandler;

public class APIConverter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(APIConverter.class);

    private LogicalPlan logicalPlan;

    private Web3j web3jClient;

    private List<SelectItem> selectItems = new ArrayList<>();
    
    private Map<String, String> aliasMapping = new HashMap<>();

    private ArrayList<List<Object>> data;

    private HashMap<String, Integer> columnNamesMap;

    public APIConverter(LogicalPlan logicalPlan, Web3j web3jClient)
    {
        this.logicalPlan = logicalPlan;
        this.web3jClient = web3jClient;
        SelectClause selectClause = logicalPlan.getQuery().getChildType(SelectClause.class, 0);
        List<SelectItem> selItems = selectClause.getChildType(SelectItem.class);
        for (SelectItem selItem : selItems)
        {
            selectItems.add(selItem);
            if (selItem.hasChildType(IdentifierNode.class))
            {

                String alias = selItem.getChildType(IdentifierNode.class, 0).getValue();
                String colName = selItem.getChildType(Column.class, 0).getChildType(IdentifierNode.class, 0).getValue();
                if (aliasMapping.containsKey(alias))
                {
                    throw new RuntimeException("Alias " + alias + " is ambiguous");
                }
                else
                {
                    aliasMapping.put(alias, colName);
                }
            }
        }
    }

    public DataFrame executeQuery()
    {
        FromItem fromItem = logicalPlan.getQuery().getChildType(FromItem.class, 0);
        Table table = fromItem.getChildType(Table.class, 0);
        String tableName = table.getChildType(IdentifierNode.class, 0).getValue();
        DataHandler dataHandler = null;
        if ("blocks".equalsIgnoreCase(tableName))
        {
            dataHandler = new BlockResultDataHandler();
        }
        else if ("transactions".equalsIgnoreCase(tableName))
        {
            dataHandler = new TransactionResultDataHandler();
        }
        else
        {
            LOGGER.error("Table : " + tableName + " does not exist. ");
            throw new RuntimeException("Table : " + tableName + " does not exist. ");
        }
        List<?> recordList = getFromTable(tableName);
         
        data = dataHandler.convertToObjArray(recordList, selectItems);
        columnNamesMap = dataHandler.getColumnNamesMap();
        DataFrame dataframe = new DataFrame(data, columnNamesMap, aliasMapping, tableName);
        return dataframe;
    }

    public <T> List<?> getFromTable(String tableName)
    {
        List<T> recordList = new ArrayList<>();

        if ("blocks".equalsIgnoreCase(tableName) || "transactions".equalsIgnoreCase(tableName))
        {
            if (logicalPlan.getQuery().hasChildType(WhereClause.class))
            {
                recordList.addAll((Collection<? extends T>) executeWithWhereClause(tableName));
            }
            else
            {
                throw new RuntimeException("Query without where clause not supported.");
            }
        }
        else
        {
            throw new RuntimeException("Unidentified table " + tableName);
        }
        return recordList;
    }

    public List<?> executeWithWhereClause(String tableName)
    {
        WhereClause whereClause = logicalPlan.getQuery().getChildType(WhereClause.class, 0);
        if (whereClause.hasChildType(FilterItem.class))
        {
            FilterItem filterItem = whereClause.getChildType(FilterItem.class, 0);
            return executeSingleWhereClause(tableName, filterItem);
        }
        else
        {
            throw new RuntimeException("Multiple where notsupported");
            // return executeMultipleWhereClause(tableName, whereClause);

        }
    }

    public List<?> executeSingleWhereClause(String tableName, FilterItem filterItem)
    {
        String filterColumn = null;

        filterColumn = filterItem.getChildType(Column.class, 0).getName();
        // TODO Implement comparator function to take other operators(for now
        // only =)
        String value = filterItem.getChildType(IdentifierNode.class, 0).getValue();
        List dataList = new ArrayList<>();

        //
        try
        {
            if ("blocks".equalsIgnoreCase(tableName))
            {
                Block block = new Block();

                if ("blocknumber".equalsIgnoreCase(filterColumn))
                {
                    block=getBlock(value);
                    dataList.add(block);
                }
                else if ("blockHash".equalsIgnoreCase(filterColumn))
                {

                    block=getBlockByHash(value.replace("'", ""));
                    dataList.add(block);

                }
                else
                {
                    LOGGER.error("Column "+filterColumn+" is not filterable");
                    throw new RuntimeException("Column "+filterColumn+" is not filterable/ Doesn't exist in the table");
                }

            }
            else if ("transactions".equalsIgnoreCase(tableName))
            {

                if ("blocknumber".equalsIgnoreCase(filterColumn))
                {
                    List<TransactionResult> transactionResult = getTransactions(value.replace("'", ""));
                    dataList = transactionResult;
                }else if("hash".equalsIgnoreCase(filterColumn)){
                   Transaction transInfo= getTransactionByHash(value.replace("'", ""));
                   dataList.add(transInfo);
                    
                }else
                {
                    LOGGER.error("Column "+filterColumn+" is not filterable/ Doesn't exist in the table");
                    throw new RuntimeException("Column "+filterColumn+" is not filterable");
                }

            }
        }
        catch (Exception e)
        {
           
            throw new RuntimeException(e.getMessage());
        }
        return dataList;
    }

    private List<TransactionResult> getTransactions(String blockNumber) throws IOException
    {
        LOGGER.info("Getting details of transactions stored in block - " + blockNumber);
        EthBlock block = web3jClient.ethGetBlockByNumber(DefaultBlockParameter.valueOf(new BigInteger(blockNumber)),
                true).send();

        return block.getBlock().getTransactions();
    }

    /**
     * Gets the block.
     *
     * @param blockNumber
     *            the block number
     * @return the block
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private Block getBlock(String blockNumber) throws IOException
    {
        LOGGER.info("Getting block - " + blockNumber + " Information ");
        EthBlock block = web3jClient.ethGetBlockByNumber(DefaultBlockParameter.valueOf(new BigInteger(blockNumber)),
                true).send();
        return block.getBlock();
    }

    /**
     * Gets the block by hash.
     *
     * @param blockHash
     *            the block hash
     * @return the block by hash
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private Block getBlockByHash(String blockHash) throws IOException
    {
        LOGGER.info("Getting  information of block with hash - " + blockHash);
        EthBlock block = web3jClient.ethGetBlockByHash(blockHash, true).send();
        return block.getBlock();
    }

    /**
     * Gets the transaction by hash.
     *
     * @param transactionHash
     *            the transaction hash
     * @return the transaction by hash
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private Transaction getTransactionByHash(String transactionHash) throws IOException
    {
        LOGGER.info("Getting information of Transaction by hash - " + transactionHash);

        Transaction transaction = web3jClient.ethGetTransactionByHash(transactionHash).send().getResult();
        return transaction;
    }

    /**
     * Gets the transaction by block hash and index.
     *
     * @param blockHash
     *            the block hash
     * @param transactionIndex
     *            the transaction index
     * @return the transaction by block hash and index
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private Transaction getTransactionByBlockHashAndIndex(String blockHash, BigInteger transactionIndex)
            throws IOException
    {
        LOGGER.info("Getting information of Transaction by blockhash - " + blockHash + " and transactionIndex"
                + transactionIndex);

        Transaction transaction = web3jClient.ethGetTransactionByBlockHashAndIndex(blockHash, transactionIndex).send()
                .getResult();
        return transaction;
    }
   
}

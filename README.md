# Introduction 
Ethereum JDBC driver implements a pure java, type 4 JDBC driver that can execute SQL queries directly on Ethereum. It uses antlr4.j grammar file to parse the query and create corresponding logical plan. This logical plan is then converted in to an optimized physical plan. The physical plan uses corresponding web3j calls to connect to Ethereum. The driver then converts the returned objects to a JDBC compliant result set and return it to the user.
It supports Querying over blocks and transactions associated to each block.

# Connection and Querying

- Check [how to connect](http://git-impetus.impetus.co.in/RND-LABS/eth-jdbc-connector/wikis/how-to-connect) to ethereum
- [Supported queries](http://git-impetus.impetus.co.in/RND-LABS/eth-jdbc-connector/wikis/jdbc-querying)

# How to Contribute

- [Contribution Guidelines](http://git-impetus.impetus.co.in/RND-LABS/eth-jdbc-connector/wikis/how-to-contribute)

About Us
========
eth-jdbc-connector is backed by Impetus Labs - iLabs. iLabs is a R&D consulting division of [Impetus Technologies](http://www.impetus.com). iLabs focuses on innovations with next generation technologies and creates practice areas and new products around them. iLabs is actively involved working on High Performance computing technologies, ranging from distributed/parallel computing, Erlang, grid softwares, GPU based software, Hadoop, Hbase, Cassandra, CouchDB and related technologies. iLabs is also working on various other Open Source initiatives.
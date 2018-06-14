# Introduction 


Ethereum JDBC driver implements a pure java, type 4 JDBC driver that executes SQL queries on Ethereum blockchain. It facilitates getting the data in and out of ethereum in JDBC compliant manner. The Ethereum JDBC driver can be used to  perform ETL, BI reporting and analytics using the familiar SQL language.

It uses [blkchn-sql-driver](http://git-impetus.impetus.co.in/RND-LABS/blkchn-sql-driver) to parse the query and create corresponding logical plan. This logical plan is then converted into an optimized physical plan. The driver extends and implements the physical plan using corresponding [web3j](https://github.com/web3j/web3j) calls to connect to Ethereum. The driver then converts the returned objects to a JDBC compliant result set and return it to the user.

# Getting Started

- [Download](http://git-impetus.impetus.co.in/RND-LABS/eth-jdbc-connector/repository/archive.zip?ref=master) sourcecode or use `git clone http://git-impetus/RND-LABS/eth-jdbc-connector.git`
- Navigate to [examples](http://git-impetus.impetus.co.in/RND-LABS/eth-jdbc-connector/tree/master/eth-jdbc-examples) folder
- Run [`Query.java`](http://git-impetus.impetus.co.in/RND-LABS/eth-jdbc-connector/blob/master/eth-jdbc-examples/src/main/java/com/impetus/blkchn/eth/Query.java) and [`Insert.java`](http://git-impetus.impetus.co.in/RND-LABS/eth-jdbc-connector/blob/master/eth-jdbc-examples/src/main/java/com/impetus/blkchn/eth/Insert.java) for quick start

To use Ethereum JDBC connector in a maven project, add the following maven dependency in your project:
 
  
  ```
  <dependency>
    <groupId>com.impetus</groupId>
    <artifactId>eth-jdbc-driver</artifactId>
    <version>${ethjdbcdriver.version}</version>
  </dependency>
  ```

Build your project with the above changes to your pom.xml.

# Connection and Querying

- Check [how to connect](http://git-impetus.impetus.co.in/RND-LABS/eth-jdbc-connector/wikis/how-to-connect) to ethereum
- [Supported queries](http://git-impetus.impetus.co.in/RND-LABS/eth-jdbc-connector/wikis/jdbc-querying)

# How to Contribute

- [Contribution Guidelines](http://git-impetus.impetus.co.in/RND-LABS/eth-jdbc-connector/blob/master/CONTRIBUTING.md)

About Us
========
eth-jdbc-connector is backed by Impetus Labs - iLabs. iLabs is a R&D consulting division of [Impetus Technologies](http://www.impetus.com). iLabs focuses on innovations with next generation technologies and creates practice areas and new products around them. iLabs is actively involved working on blockchain technologies, neural networking, distributed/parallel computing and advanced analytics using spark and big data ecosystem. iLabs is also working on various other Open Source initiatives.

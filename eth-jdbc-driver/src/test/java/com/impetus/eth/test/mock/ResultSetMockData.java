package com.impetus.eth.test.mock;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.impetus.blkch.sql.DataFrame;
import com.impetus.eth.jdbc.EthResultSet;

public class ResultSetMockData {

    public static ResultSet returnEthResultSetAll() {
        List<List<Object>> data = new ArrayList<List<Object>>();
        List<Object> record0 = new ArrayList<Object>();
        record0.add("0xbd8c4b656c2d2c002743a96297dc5fea155293c123f8c55f8fc127f250f3312f");
        record0.add(1652339);
        record0.add(null);
        record0.add("0x38eea66d6576989b1c7db3a872799d339f61527c");
        record0.add("0x15f90");
        record0.add("23000000000");
        record0.add("0x6c0eb486074675f3c02c11321e0e7ba1fae0095b2ea2a79d4f40c44d4b9537a1");
        record0.add("0x");
        record0.add("51");
        record0.add(null);
        record0.add("0xc05cd02350690700ec51ce299701baa3f457953a78d953fcb31778d74c7f2fd1");
        record0.add(null);
        record0.add("0x4258ef10c5abf027d191317151f97490319c401a9ac65f8cb34d5c70667afeee");
        record0.add("0xa76cd046cf6089fe2adcf1680fcede500e44bacd");
        record0.add(0);
        record0.add(41);
        record0.add("50000000000000000000");
        data.add(record0);

        List<Object> record1 = new ArrayList<Object>();
        record1.add("0xbd8c4b656c2d2c002743a96297dc5fea155293c123f8c55f8fc127f250f3312f");
        record1.add("1652339");
        record1.add(null);
        record1.add("0x4d552898d5a55f4ee4f73dce164fd64ba8d93cfb");
        record1.add("0x15f90");
        record1.add("23000000000");
        record1.add("0x2f52be8c1668793bf5b62a6d80b440b5b831243e8757bc76f80a5b2d77975c65");
        record1.add("0xe100bc7d00000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000028494e5625324632303137303833312532465856494925324656494949253246313031343038363330000000000000000000000000000000000000000000000000");
        record1.add("386");
        record1.add(null);
        record1.add("0xf7fcb1b5a20ddd00e4cf36be3c130c8e828487d72271fb73236f40efb9ee4411");
        record1.add(null);
        record1.add("0x58058f76d7f79d20c35754e09bef932aa8b82f56e66d19016c1cfe5668f79392");
        record1.add("0x5f81dc51bdc05f4341afbfa318af5d82c607acad");
        record1.add(1);
        record1.add(42);
        record1.add(0);
        data.add(record1);

        List<Object> record2 = new ArrayList<Object>();
        record2.add("0xbd8c4b656c2d2c002743a96297dc5fea155293c123f8c55f8fc127f250f3312f");
        record2.add("1652339");
        record2.add(null);
        record2.add("0x287a544dee8da5b7eb64da3b0d95ea4040b40f15");
        record2.add("0x4630c0");
        record2.add("21000000000");
        record2.add("0x1c277cda5cfdbabafad1bd23cc6106cd94eded8d118a118312e871cac3e286e0");
        record2.add("0xa3ea478f00000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000064000000000000000000000000cb672b5f26280f91f7aef18f941f5a5453245f37000000000000000000000000184fe4f64a33cac9f16ca96e9d1ca7f877414d9900000000000000000000000000064ad99981867e0c764acf04019be7457cdbc0000000000000000000000000cd21481df570cc0f5ae90a0ad7f30d8e22b765090000000000000000000000006cd3c98e754f337c8c0944eb0a050ec3573d062a0000000000000000000000002a27c53a840ff0cabf5d0127a704bc725853743f00000000000000000000000059274949f2723c3a5622df1e469a51d14a7e2a3a000000000000000000000000a72d74fa25ddb1b383631913a1d7b45c45dfc1a50000000000000000000000003c7839b400ed9830e97d75c3a1371b5cca7d74c30000000000000000000000008da59f597b3e084222ffe1792a7bccdeb01c849b000000000000000000000000ab0ad7a759d7582b0e7fc5e258cba5b279c1b8d1000000000000000000000000c8cf67d72b60549f3ef456a6b6150e517af91ccf00000000000000000000000082758d37a59d4ac5b0d8bfc71ce096af6d63cd7d0000000000000000000000009316a936c829a4490f9bdbf9d89a3006c66d2c1b000000000000000000000000f29d4f3213a32326c9d6f0dc19748a48d179c99800000000000000000000000042b597aa408bc0ae866af3a6f30f9682e4c7219c000000000000000000000000189af4120b85bafcb4da0b66ba5c403be2984893000000000000000000000000bb556ccba4f044ee0f68b64eca9713eefba15af90000000000000000000000004bc1920b74bef6deedc1ff3d4204b62a29daaa11000000000000000000000000bf40481670139ae379d74415fe8dfeef3817f892000000000000000000000000b2409871f10f7e51cad926b72e7fa974b3ad6586000000000000000000000000c4508a4910be065b4c61808f3bf7ffde27d9110f000000000000000000000000c6f08e34d6fae9c32d89b837306caf8b87950970000000000000000000000000a4b0fc2f5af2ac39b80eb214160962496e9dede80000000000000000000000004e62d92cbc923f02bfaff6cee0104cf9e87b08a9000000000000000000000000faaaa3cdb8c9d300513b9bd2daf7c8b1563215a4000000000000000000000000289d830ae402ac13af8b749237bd060bcedaa56a0000000000000000000000002e5d5cbf7f6a63787e7aa5bfd66c3da4b8efb85c000000000000000000000000cfe5a1ad5d9aa8cbc2aaec2476a0184ed514a3700000000000000000000000000901862599d11852918664952d3c055908cbf7a700000000000000000000000000329e92ed6fd0a0ef76da9c476d51faeb6b26a000000000000000000000000091e7626e23a6af6e7ab63f07e2fc189cc23e8bac000000000000000000000000780561dca3cfe7625afe7b0d589a450a950c5c030000000000000000000000008eb7d07c810d79d977f3074d6b55a026b086cb8c0000000000000000000000000edc884163190b1df75ba620c058362532a3bd19000000000000000000000000c4ad0b9174dbe86cbfb53a218074607cc261df6d00000000000000000000000052bc44d5378309ee2abf1539bf71de1b7d7be3b5000000000000000000000000fe6becd476fa6c2d7546f4b0872f7dc9259e11a2000000000000000000000000013197e00918f56176f677a3498593004c89713a000000000000000000000000430093cab646d6a0939b102a8c95382a926d61f1000000000000000000000000d843ba10c285d93e19f599e4311aa3bef7cb0b1c0000000000000000000000009e51530988e4601566efab6926c4db2b1cb148c9000000000000000000000000d8f4546cd7aa4a300d338719bb71b39466afd0b500000000000000000000000082848da79a5f443b98455da1b5b832d12af14070000000000000000000000000caee11929f06002d096c4dcb8ea061323e21e724000000000000000000000000009e4d7424969c30d5cf0ecab01aacefba853992000000000000000000000000873c544b8870d6fdaa4a16cfa0c9fcde7ec1947700000000000000000000000017f9467c391cb41fabeff766070e094ecb95762400000000000000000000000000af36c43ab459a4eacf93467c400ac45bcbc1240000000000000000000000006df151b5339b90e63cffbcb78ee851ebb16dfea10000000000000000000000002a5cb52afb43d2326a97360d77a0935f24670fbe00000000000000000000000082719e60cfcfbbbbece69903ebccef79453fb9a200000000000000000000000002c7b3a5a42142b519ddd8b7070e798911a81a6f00000000000000000000000039465983e8415b9bffbc6163df9eccfeeab8edef0000000000000000000000003cfd06e4a1a94dcc230b4e30f9af0dc05a60e8ba000000000000000000000000f3f5ed2ac8ad77a479d9134f8541d5918cd0c14f000000000000000000000000924990ccfa6238ef43d72e9a52d5c9a4d6901b27000000000000000000000000cfd4539cf5a18fa33efcabefa9b93812568bda05000000000000000000000000003a5ab1bceda1dda9cd41abcc436c0bfe44dba7000000000000000000000000cd192fe7023897ff5b37a4be6d8e3de304a9a8f8000000000000000000000000f4423d4a53a8b63b0f962e2ec887055df2ee8e6900000000000000000000000088da387eaa1d4cac7eaf21cc8d707dfc0e40f4300000000000000000000000006fa2229abc19be17f4108ddfcb8a3ac3366216c90000000000000000000000001e97fd908a9222a6fc00ddf7fd91dfe4e228e9dd000000000000000000000000c9c303f775fb054e6cafd63ac753ab9eacb94df9000000000000000000000000c3af6926876fca701c4da1f2dd1ce5261aa7e9cf00000000000000000000000081a663ad2c68d77dfe11eb7a7c0e0574e4ae8c1100000000000000000000000089083a4d3cd1f1e2403190f7ad1d5fbb93c7c803000000000000000000000000f09f23c6422ded955af8dbc920697f5b43ee3e67000000000000000000000000a585db319d188d79103f2233a1d7f2fe5a165a6600000000000000000000000073fab916da49f62b9fc87f542d5b2d59bd8c3494000000000000000000000000f48499d9ae1a7ef2e95ec43ed96d2c537f49528900000000000000000000000055f3a5e90c93bb26e60c1c46df72f72abbb7d4dc00000000000000000000000014dd2b0684c312768183c1b1aff4e86d99436265000000000000000000000000b577301c2a5cd78fb8cc0812c17dbe4f83c658d80000000000000000000000001deda58f64751b9b02f739641fde3b43b5a509e10000000000000000000000006503701e26ca0a2eb71a21e29cd561d5d7315dc8000000000000000000000000afd8635b081c6b73a47ff525fe733ec61cefbf9f000000000000000000000000290dbf7eb51324266f0668675a2a230d250b293c000000000000000000000000b31f2fa364d45f701d33100258dfd5eef73fd7d90000000000000000000000004dd7690a7c55a0d20861a00c33234db7ff3b724b000000000000000000000000489e9cfede1e73d5768bb7f31c9ffbc0e1d31b6b000000000000000000000000340ddb1b291e70fb5acb3837ee4cfa6b1688791d0000000000000000000000004ef23e179b68cd54313ac8fc7608855435e9d3e800000000000000000000000020429458f5645dff4522b3adb23116d1c843bf1800000000000000000000000061c7be728dcc1235826fcded4060b1ee2c46e41a000000000000000000000000b002c989a1a6cc4d92a8c472619a7803ab7850c000000000000000000000000024838c2ca8ebc1ec5781de039e650ab9fab7a795000000000000000000000000211ad02c2d6aaf6552bc6d38bae7b0e763a9dae000000000000000000000000087bc0d9c737767228d192dd2fa86a064871246400000000000000000000000005ec9e478ee98c7a4374b860abe2e22a93f838a1a0000000000000000000000000ac7967d001d2667d9268da124ef45b06d1757b2000000000000000000000000706cd30ce4f25da69bafd938f45835475f58ada60000000000000000000000007add7bf51bb0f63952f65887eec54db253e58f93000000000000000000000000799cbe409c4c06d50bc4e2da810d80c40034cae5000000000000000000000000120f58f6ec511e1aba2d6ee9ea0f080525de90cc00000000000000000000000082f60083f6b5b38c95803d62f4188344d3dab4af0000000000000000000000007e35bff13a1966efe0160252bedc6569fbbdc137000000000000000000000000a31dc8056000ffd48417446a507d9302de8d4ea10000000000000000000000005efbf6dc09ec9ccac40996bb63f98b0b45986cfc");
        record2.add(296);
        record2.add(null);
        record2.add("0x1af44a619000c9310c506f2925c7270350a61bb67cd941db660e861ae54b7c60");
        record2.add(null);
        record2.add("0x7a8e84273cfd17447928a0c7a1d574a491454246838a7bdfe6df623d1cd4c907");
        record2.add("0x7ce38f22a5ad6cf05b5a6246f795e6ce4a34e313");
        record2.add(2);
        record2.add(41);
        record2.add(0);
        data.add(record2);
        HashMap<String, Integer> columnNamesMap = new HashMap<>();
        columnNamesMap.put("blockhash", 0);
        columnNamesMap.put("blocknumber", 1);
        columnNamesMap.put("creates", 2);
        columnNamesMap.put("from", 3);
        columnNamesMap.put("gas", 4);
        columnNamesMap.put("gasprice", 5);
        columnNamesMap.put("hash", 6);
        columnNamesMap.put("input", 7);
        columnNamesMap.put("nonce", 8);
        columnNamesMap.put("publickey", 9);
        columnNamesMap.put("r", 10);
        columnNamesMap.put("raw", 11);
        columnNamesMap.put("s", 12);
        columnNamesMap.put("to", 13);
        columnNamesMap.put("transactionindex", 14);
        columnNamesMap.put("v", 15);
        columnNamesMap.put("value", 16);
        Map<String, String> aliasMapping = new HashMap<String, String>();
        DataFrame df = new DataFrame(data, columnNamesMap, aliasMapping);
        return new EthResultSet(df, 0, 0, "transaction");

    }

    public static ResultSet returnEthResultSetGroupBy() {
        List<List<Object>> data = new ArrayList<List<Object>>();
        List<Object> record0 = new ArrayList<Object>();
        record0.add(1);
        record0.add("0xde63fb4975f70c73f826be406a8d5cb8ad4e73f5");
        data.add(record0);

        List<Object> record1 = new ArrayList<Object>();
        record1.add(1);
        record1.add("0x5f81dc51bdc05f4341afbfa318af5d82c607acad");
        data.add(record1);

        List<Object> record2 = new ArrayList<Object>();
        record2.add(1);
        record2.add("0x8144c67b144a408abc989728e32965edf37adaa1");
        data.add(record2);

        List<Object> record3 = new ArrayList<Object>();
        record3.add(1);
        record3.add("0xa76cd046cf6089fe2adcf1680fcede500e44bacd");
        data.add(record3);

        List<Object> record4 = new ArrayList<Object>();
        record4.add(1);
        record4.add("0x29bd8851748cc14d52c30e2ae350a071b471d6b5");
        data.add(record4);

        List<Object> record5 = new ArrayList<Object>();
        record5.add(2);
        record5.add("0x7ce38f22a5ad6cf05b5a6246f795e6ce4a34e313");
        data.add(record5);

        HashMap<String, Integer> columnNamesMap = new HashMap<>();
        columnNamesMap.put("count(to)", 0);
        columnNamesMap.put("to", 1);
        Map<String, String> aliasMapping = new HashMap<String, String>();
        DataFrame df = new DataFrame(data, columnNamesMap, aliasMapping);
        return new EthResultSet(df, 0, 0, "transactions");

    }

    public static ResultSet returnEthResultSetMultipleBlocks() {
        List<List<Object>> data = new ArrayList<List<Object>>();
        List<Object> record0 = new ArrayList<Object>();
        record0.add("1652339");
        record0.add("0xbd8c4b656c2d2c002743a96297dc5fea155293c123f8c55f8fc127f250f3312f");
        record0.add("0xa76cd046cf6089fe2adcf1680fcede500e44bacd");
        record0.add("50000000000000000000");
        record0.add("23000000000");
        data.add(record0);

        List<Object> record1 = new ArrayList<Object>();
        record1.add("1652339");
        record1.add("0xbd8c4b656c2d2c002743a96297dc5fea155293c123f8c55f8fc127f250f3312f");
        record1.add("0x5f81dc51bdc05f4341afbfa318af5d82c607acad");
        record1.add("0");
        record1.add("23000000000");
        data.add(record1);

        List<Object> record2 = new ArrayList<Object>();
        record2.add("1652339");
        record2.add("0xbd8c4b656c2d2c002743a96297dc5fea155293c123f8c55f8fc127f250f3312f");
        record2.add("0x7ce38f22a5ad6cf05b5a6246f795e6ce4a34e313");
        record2.add("0");
        record2.add("21000000000");
        data.add(record2);

        List<Object> record3 = new ArrayList<Object>();
        record3.add("1652340");
        record3.add("0xf14996d3b2cd1a4d609c0261f645b1e632b6ac1356431da0b8a47e8c6de1492c");
        record3.add("0xde63fb4975f70c73f826be406a8d5cb8ad4e73f5");
        record3.add("0");
        record3.add("23000000000");
        data.add(record3);

        List<Object> record4 = new ArrayList<Object>();
        record4.add("1652340");
        record4.add("0xf14996d3b2cd1a4d609c0261f645b1e632b6ac1356431da0b8a47e8c6de1492c");
        record4.add("0x8144c67b144a408abc989728e32965edf37adaa1");
        record4.add("50000000000000000000");
        record4.add("23000000000");
        data.add(record4);

        List<Object> record5 = new ArrayList<Object>();
        record5.add("1652340");
        record5.add("0xf14996d3b2cd1a4d609c0261f645b1e632b6ac1356431da0b8a47e8c6de1492c");
        record5.add("0x29bd8851748cc14d52c30e2ae350a071b471d6b5");
        record5.add("50000000000000000000");
        record5.add("23000000000");
        data.add(record5);

        List<Object> record6 = new ArrayList<Object>();
        record6.add("1652340");
        record6.add("0xf14996d3b2cd1a4d609c0261f645b1e632b6ac1356431da0b8a47e8c6de1492c");
        record6.add("0x7ce38f22a5ad6cf05b5a6246f795e6ce4a34e313");
        record6.add("2600000000000000000");
        record6.add("21000000000");
        data.add(record6);

        HashMap<String, Integer> columnNamesMap = new HashMap<>();
        columnNamesMap.put("blocknumber", 0);
        columnNamesMap.put("blockhash", 1);
        columnNamesMap.put("to", 2);
        columnNamesMap.put("value", 3);
        columnNamesMap.put("gasprice", 4);
        Map<String, String> aliasMapping = new HashMap<String, String>();
        DataFrame df = new DataFrame(data, columnNamesMap, aliasMapping);
        return new EthResultSet(df, 0, 0, "transaction");

    }

    public static ResultSet returnEthResultSetOrderBy() {
        List<List<Object>> data = new ArrayList<List<Object>>();
        List<Object> record0 = new ArrayList<Object>();
        record0.add("1652339");
        record0.add("0xbd8c4b656c2d2c002743a96297dc5fea155293c123f8c55f8fc127f250f3312f");
        record0.add("0xa76cd046cf6089fe2adcf1680fcede500e44bacd");
        record0.add("50000000000000000000");
        record0.add("23000000000");
        data.add(record0);

        List<Object> record1 = new ArrayList<Object>();
        record1.add("1652339");
        record1.add("0xbd8c4b656c2d2c002743a96297dc5fea155293c123f8c55f8fc127f250f3312f");
        record1.add("0x5f81dc51bdc05f4341afbfa318af5d82c607acad");
        record1.add("0");
        record1.add("23000000000");
        data.add(record1);

        List<Object> record2 = new ArrayList<Object>();
        record2.add("1652340");
        record2.add("0xf14996d3b2cd1a4d609c0261f645b1e632b6ac1356431da0b8a47e8c6de1492c");
        record2.add("0xde63fb4975f70c73f826be406a8d5cb8ad4e73f5");
        record2.add("0");
        record2.add("23000000000");
        data.add(record2);

        List<Object> record3 = new ArrayList<Object>();
        record3.add("1652340");
        record3.add("0xf14996d3b2cd1a4d609c0261f645b1e632b6ac1356431da0b8a47e8c6de1492c");
        record3.add("0x8144c67b144a408abc989728e32965edf37adaa1");
        record3.add("50000000000000000000");
        record3.add("23000000000");
        data.add(record3);

        List<Object> record4 = new ArrayList<Object>();
        record4.add("1652340");
        record4.add("0xf14996d3b2cd1a4d609c0261f645b1e632b6ac1356431da0b8a47e8c6de1492c");
        record4.add("0x29bd8851748cc14d52c30e2ae350a071b471d6b5");
        record4.add("50000000000000000000");
        record4.add("23000000000");
        data.add(record4);

        List<Object> record5 = new ArrayList<Object>();
        record5.add("1652339");
        record5.add("0xbd8c4b656c2d2c002743a96297dc5fea155293c123f8c55f8fc127f250f3312f");
        record5.add("0x7ce38f22a5ad6cf05b5a6246f795e6ce4a34e313");
        record5.add("0");
        record5.add("21000000000");
        data.add(record5);

        List<Object> record6 = new ArrayList<Object>();
        record6.add("1652340");
        record6.add("0xf14996d3b2cd1a4d609c0261f645b1e632b6ac1356431da0b8a47e8c6de1492c");
        record6.add("0x7ce38f22a5ad6cf05b5a6246f795e6ce4a34e313");
        record6.add("2600000000000000000");
        record6.add("21000000000");
        data.add(record6);

        HashMap<String, Integer> columnNamesMap = new HashMap<>();
        columnNamesMap.put("blocknumber", 0);
        columnNamesMap.put("blockhash", 1);
        columnNamesMap.put("to", 2);
        columnNamesMap.put("value", 3);
        columnNamesMap.put("gasprice", 4);
        Map<String, String> aliasMapping = new HashMap<String, String>();
        DataFrame df = new DataFrame(data, columnNamesMap, aliasMapping);
        return new EthResultSet(df, 0, 0, "transaction");

    }

}
package com.impetus.blkchn.eth;

import com.typesafe.config.*;
import java.io.File;
import java.util.Objects;

public class ReadConfig {
    public static String keystorePath = "path";
    public static String keystorePassword = "keystore_password";
    public static String address = "address";
    public static String smartContractAddress = "smartContractAddress";

    public static void loadConfig(){
        Config config = ConfigFactory.load();
        Config app = config.getConfig("example");
        String keystorePathFile = app.getString("keystore_path");
        ClassLoader classLoader = ReadConfig.class.getClassLoader();
        keystorePath = new File(Objects.requireNonNull(classLoader.getResource(keystorePathFile)).getFile()).getPath();
        keystorePassword = app.getString("keystore_password");
        smartContractAddress= app.getString("smartContractAddress");
        address = app.getString("address");
    }
}

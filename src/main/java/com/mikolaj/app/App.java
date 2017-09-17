package com.mikolaj.app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;


/**
 * smaller
 *
 */
public class App {

    public static void main(String[] args) throws Exception {
        // Start calculating execution time
        long startTime = System.currentTimeMillis();
        String execDate = MyUtils.getCurrentDateTime();
        String programName;

        Configuration conf = new Configuration();
        /** Find here all configuration options: https://github.com/ZuInnoTe/hadoopcryptoledger/wiki/Hadoop-File-Format **/
        conf.set("hadoopcryptoledger.bitcoinblockinputformat.filter.magic","F9BEB4D9");
        //conf.setBoolean("fs.hdfs.impl.disable.cache", true); //not sure, for appending in HDFS

        // Parse blockchain into PostgreSQL database
        //ToolRunner.run(new ParsePostgresRawDriver(), args);
        //programName = "ParsePostgresRawMapReduce";

        // Parse blockchain into HDFS text files (per taks)
        ToolRunner.run(new ParseHdfsRawTaskDriver(), args);
        programName = "ParseHdfsRawTaskMapReduce";

        // Finish calculating execution time
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        System.out.println("\nTotal execution time: " + String.valueOf(totalTime) + "\n");

        // Save report on disk
        int filesCount = MyUtils.countInputHdfsFiles(conf, args[args.length - 2]); // count number of hdfs input files
        MyUtils.generateReport(programName, execDate, filesCount, totalTime);

        //System.exit(exitCode);
    }
    //a
}

package com.hadoop.executor;

 public class Constants {

    public static String outputFilePath = "/output/";
    public static String HDFS_URI = "hdfs://localhost:9000";
    // file system location
    public static String FS_INPUT_SOURCE = "";
    public static String FS_OUTPUT_LOCATION = "";

    // HDFS file location
    public static String HDFS_INPUT_SOURCE = "";
    public static String HDFS_OUTPUT_LOCATION = "";

    // job string
    public static final String FILE_LOAD_TO_HADOOP = "loadToHDFS"; // load to hadoop
    public static final String WORD_COUNTER_JOB = "wordCounter"; // process job
    public static final String FILE_LOAD_TO_FILE_SYSTEM = "loadToFS"; // download to file system from hadoopFS
}

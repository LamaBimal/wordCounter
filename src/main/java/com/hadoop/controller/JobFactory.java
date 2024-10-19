package com.hadoop.controller;

import com.hadoop.executor.Constants;
import com.hadoop.job.HDFSFileDownloader;
import com.hadoop.job.HDFSFileUploader;
import com.hadoop.job.impl.IRunner;
import com.hadoop.job.wordcounter.WordCounter;

public class JobFactory {

    public static IRunner getJob(String jobName){
        IRunner jobRunner;
        switch (jobName){
            case Constants.FILE_LOAD_TO_HADOOP:
                // call function to load in Hadoop FS
                jobRunner = new HDFSFileUploader();
                break;
            case Constants.WORD_COUNTER_JOB:
                // function call for word counter
                jobRunner = new WordCounter();
                break;
            case Constants.FILE_LOAD_TO_FILE_SYSTEM:
                // function call for file upload to FS
                jobRunner = new HDFSFileDownloader();
                break;
            default:
                jobRunner = new HDFSFileUploader();
                break;
        }
        return jobRunner;
    }
}

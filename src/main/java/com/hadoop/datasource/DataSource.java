package com.hadoop.datasource;

public class DataSource {
    String jobName;
    String inputSourcePath;

    public String getInputSourcePath() {
        return inputSourcePath;
    }

    public void setInputSourcePath(String inputSourcePath) {
        this.inputSourcePath = inputSourcePath;
    }

    public String getOutputFilePath() {
        return outputFilePath;
    }

    public void setOutputFilePath(String outputFilePath) {
        this.outputFilePath = outputFilePath;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    String outputFilePath;
}

package com.hadoop.executor;

import java.io.File;

import com.hadoop.controller.JobFactory;
import com.hadoop.datasource.DataSource;
import com.hadoop.exception.InvalidParameterException;
import com.hadoop.job.impl.IRunner;

public class JobExecutor {
    DataSource dataSource;

    public JobExecutor(){
        dataSource = new DataSource();
    }
    public static void main(String[] args) throws NullPointerException{
        /*try {
            args[0]= "/input_wordcount/New_York_cars.csv";
            JobConf conf = new JobConf(JobExecutor.class);
            conf.setJobName("WordCount");
            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(IntWritable.class);
            conf.setMapperClass(wordCounterMapper.class);
            conf.setCombinerClass(wordCounterReducer.class);
            conf.setReducerClass(wordCounterReducer.class);
            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);

            FileInputFormat.setInputPaths(conf,new Path(args[0]));
            String inputFileName = extractFileName(args[0]);
            String outputFilePath = generateOutputPath(inputFileName);
            System.out.println(">> Generated output file path >> "+outputFilePath);
            FileOutputFormat.setOutputPath(conf,new Path(outputFilePath));
            JobClient.runJob(conf);

        } catch (Exception ex){
          ex.printStackTrace();
        }*/

        try{
            JobExecutor executor = new JobExecutor();
            executor.readParameters(args);
            IRunner job = JobFactory.getJob(executor.dataSource.getJobName());
            DataSource datasource = executor.dataSource;
            job.run(datasource.getInputSourcePath(),datasource.getOutputFilePath());

        } catch (InvalidParameterException exception){
            System.out.println(exception.getMessage());
        }

    }


    public void readParameters(String[] args) throws InvalidParameterException{
        String key = "";
        for (String arg: args){
            if(key.toLowerCase().contains("job") ){
                System.out.println(">>  job Name :"+ arg);
                dataSource.setJobName(arg);
            } else if (key.toLowerCase().contains("input")){
                System.out.println(">> input :"+arg);
                dataSource.setInputSourcePath(arg);
            } else if (key.toLowerCase().contains("output")){
                System.out.println(">> output :"+arg);
                dataSource.setOutputFilePath(arg);
            }
            if(arg.contains("--")){
                key = arg;
               // System.out.println(">> Key >> "+key);
            } else {
                key = "";
            }
        }
        if(dataSource.getJobName().isEmpty()){
            throw new InvalidParameterException("job parameter is missing.");
        } else if(dataSource.getInputSourcePath().isEmpty()){
            throw  new InvalidParameterException("input parameter is missing.");
        } else if (dataSource.getOutputFilePath().isEmpty()){
            throw new InvalidParameterException("output parameter is missing.");
        } else {
            System.out.println("Parameters has been validated Successfully.");
        }
    }

    public static String executeWordCounter(){
        return null;
    }

    public static String extractFileName(String filePath){

        try{
            File file = new File(filePath);
            // Extract the file name without the extension
            String fileName = file.getName();
            int dotIndex = fileName.lastIndexOf('.');

            if (dotIndex > 0 && dotIndex < fileName.length() - 1) {
                fileName = fileName.substring(0, dotIndex); // Remove extension
            }
            System.out.println("Extracted file name: " + fileName);
            return fileName;
        } catch (Exception ex ){
            ex.printStackTrace();
        }
        return null;
    }
}

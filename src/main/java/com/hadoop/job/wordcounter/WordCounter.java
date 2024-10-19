package com.hadoop.job.wordcounter;

import com.hadoop.executor.JobExecutor;
import com.hadoop.job.impl.IRunner;
import com.hadoop.utils.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class WordCounter implements IRunner {
    @Override
    public void run(String inputSource, String outputPath) {
        try {
            //args[0]= "/input_wordcount/New_York_cars.csv";
            JobConf conf = new JobConf(JobExecutor.class);
            conf.setJobName("WordCount");
            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(IntWritable.class);
            conf.setMapperClass(wordCounterMapper.class);
            conf.setCombinerClass(wordCounterReducer.class);
            conf.setReducerClass(wordCounterReducer.class);
            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);

            FileInputFormat.setInputPaths(conf,new Path(inputSource));
            String inputFileName = FileUtils.extractFileName(inputSource);
            String outputFilePath = FileUtils.generateOutputPath(inputFileName);
            System.out.println(">> Generated output file path >> "+outputFilePath);
            FileOutputFormat.setOutputPath(conf,new Path(outputFilePath));
            JobClient.runJob(conf);

        } catch (Exception ex){
            ex.printStackTrace();
        }
    }
}

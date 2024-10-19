package com.hadoop.job;

import com.hadoop.job.impl.IRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.net.URI;

import static com.hadoop.executor.Constants.HDFS_URI;

public class HDFSFileDownloader implements IRunner {
    public static void main(String[] args) {
        String hdfsFilePath = HDFS_URI+"/user/hadoop/input_text_oct_16.txt";  // HDFS file path
        String localFilePath = "/home/hadoop/output_file/input_text_oct_16.txt";  // Local file system path

        HDFSFileDownloader downloader = new HDFSFileDownloader();
        downloader.run(hdfsFilePath,localFilePath);
    }

    @Override
    public void run(String hdfsSourceLocation, String localFSOutputPath) {
        Configuration conf = new Configuration();
        FileSystem hdfs = null;

        try {
            // Get the HDFS instance (use the correct URI for your HDFS)
            hdfs = FileSystem.get(URI.create(hdfsSourceLocation), conf);

            // Copy file from HDFS to the local file system
            Path hdfsPath = new Path(hdfsSourceLocation);
            Path localPath = new Path(localFSOutputPath);

            hdfs.copyToLocalFile(hdfsPath, localPath);

            System.out.println("File downloaded to local file system: " + localFSOutputPath);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (hdfs != null) {
                    hdfs.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
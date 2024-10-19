package com.hadoop.job;

import com.hadoop.job.impl.IRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

import static com.hadoop.executor.Constants.HDFS_URI;

public class HDFSFileUploader implements IRunner {

    public static void main(String[] args) throws IOException {
        // HDFS URI
        String hdfsUri = "hdfs://localhost:9000";  // Change to your HDFS URI

        // External/local file path
        String localFilePath = "/home/hadoop/input_file/input_text_oct_16.txt";

        // HDFS destination path
        String hdfsDestinationPath = "/user/hadoop/input_text_oct_16.txt"; // Destination in HDFS

        HDFSFileUploader uploader = new HDFSFileUploader();
        uploader.run(localFilePath,hdfsDestinationPath);

    }

    @Override
    public void run(String localFSInputSource, String hdfsSourcePath) {
        // Hadoop Configuration
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", HDFS_URI);

        // Get the HDFS filesystem object
        try (FileSystem fs = FileSystem.get(URI.create(HDFS_URI), conf)){

            // Upload the file from local to HDFS
            Path localPath = new Path(localFSInputSource);
            Path hdfsPath = new Path(hdfsSourcePath);

            // Copy file from local to HDFS
            fs.copyFromLocalFile(localPath, hdfsPath);
            System.out.println("File uploaded to " + hdfsSourcePath);
        } catch (IOException e) {
            System.err.println("IOException Occurred during file upload: " + Arrays.toString(e.getStackTrace()));
        } catch (Exception e){
            System.out.println("Exception Occurred during file upload: "+ Arrays.toString(e.getStackTrace()));
        }
    }
}


package com.hadoop.utils;

import com.hadoop.executor.Constants;

import java.io.File;

public class FileUtils {
    public static String generateOutputPath(String inputFileName){

        long currentTimeStamp = System.currentTimeMillis();
        System.out.println(">>> input source "+inputFileName+" >> current timestamp >> "+currentTimeStamp);

        return Constants.outputFilePath+"output_"+inputFileName+"_"+currentTimeStamp+".txt";
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

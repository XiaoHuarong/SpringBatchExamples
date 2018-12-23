package com.flower.springbatch.batchjob.util;

import java.io.File;
import java.io.IOException;

public class FileUtil {


    public static boolean checkOrCreateFile(String filePath, boolean isDirectory) {
        try {
            File file = new File(filePath);
            if (!file.exists() && !isDirectory) {
                file.createNewFile();
                return true;
            } else if (!file.exists() && isDirectory){
                file.mkdirs();
                return true;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }
}
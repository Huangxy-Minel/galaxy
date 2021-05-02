package galaxy.galaxyio;

import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
    Function: Read data from HDFS
 */

 public class Reader {
     /**
        Get a file from HDFS
        Para: 
            filePath: file path at HDFS
            localPath: local path that save the file
            conf: YARN configuration
      */
      public static void copyFileFromHDFS (Path filePath, Path localPath, YarnConfiguration conf) throws Exception {
          // ----------------Init instance of fs----------------
            FileSystem fs = FileSystem.get(conf);
            fs.copyToLocalFile(filePath, localPath);
      }
 }
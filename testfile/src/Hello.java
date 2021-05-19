package galaxy.testfile;

import java.io.FileWriter;
import java.io.IOException;
import java.lang.Thread;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

public class Hello{
    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        System.out.println("Hello World!");
        Thread.currentThread().sleep(5000);
        long endTime = System.currentTimeMillis();
        System.out.println("Container runtime: " + (endTime - startTime) + "ms");

        // // ----------------Init instance of fs----------------
        // YarnConfiguration conf = new YarnConfiguration();
        // FileSystem fs = FileSystem.get(conf);
        // Path helloPath = new Path(fs.getHomeDirectory(), "hello");
        // // ----------------Create Hello----------------
        // FSDataOutputStream helloFile = fs.create(helloPath);
        // helloFile.writeBytes("Hello World!\n");
    }
}

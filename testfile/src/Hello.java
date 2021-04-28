package galaxy.testfile;

import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

public class Hello{
    public static void main(String[] args) throws Exception {
        System.out.println("Hello World!");

        // ----------------Init instance of fs----------------
        YarnConfiguration conf = new YarnConfiguration();
        FileSystem fs = FileSystem.get(conf);
        Path helloPath = new Path(fs.getHomeDirectory(), "hello");
        // ----------------Create Hello----------------
        FSDataOutputStream helloFile = fs.create(helloPath);
        helloFile.writeBytes("Hello World!\n");
    }
}
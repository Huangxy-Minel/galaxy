package galaxy.dataprocess.mapreduce.wordcount;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.FileWriter;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import galaxy.galaxyio.Reader;

public class Redicer {
    /**
        Function: complete a Redice task, run in container
        Para:
            arg[0]: output dir
                Type: String
     */
    public static void main(String[] args) throws Exception {
        System.out.println("Enter Map function");

        // ----------------Init----------------
        Path outputDir = new Path(args[0]);
        YarnConfiguration conf = new YarnConfiguration();
        FileSystem fs = FileSystem.get(conf);
        FileInputStream inputStream = new FileInputStream("random_text");
        BufferedReader input = new BufferedReader(new InputStreamReader(inputStream));
        Path outputPath = new Path(args[0], "random_text_0_reduce");
        fs.mkdirs(outputDir);

        // ----------------Read input file----------------
        // FSDataOutputStream mapout = fs.create(outputPath);
        String line = null;
        while ((line = input.readLine()) != null) {
            for (String word : line.split(" ")){
                mapout.writeBytes(word + " 1\n");
            }
        }
        inputStream.close();
        input.close();
    }
}
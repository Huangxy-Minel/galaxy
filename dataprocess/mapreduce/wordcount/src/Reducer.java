package galaxy.dataprocess.mapreduce.wordcount;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.FileWriter;

import com.google.common.collect.Maps;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import java.lang.Integer;
import java.util.Map;
import java.util.HashMap;

import galaxy.galaxyio.Reader;

public class Reducer {
    /**
        Function: complete a Redice task, run in container
        Para:
            arg[0]: output dir
                Type: String
            args[1]: num of mapout files
                Type: String
     */
    public static void main(String[] args) throws Exception {
        System.out.println("Enter Reduce function.");

        //  ----------------Test----------------
        long startTime = System.currentTimeMillis();
        //  ----------------Test----------------

        // ----------------Init----------------
        Path outputDir = new Path(args[0]);
        int mapoutNum = Integer.parseInt(args[1]);
        YarnConfiguration conf = new YarnConfiguration();
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(args[0], "random_text_reduce");
        // fs.mkdirs(outputDir);

        // ----------------Read input file and calculate sum----------------
        System.out.println("Reading map files.");
        Map<String, Integer> wordsSumMap = new HashMap<String, Integer>();       // key: word, value: sum
        String line = null;
        for (int i = 0; i < mapoutNum; i++) {
            FileInputStream inputStream = new FileInputStream("mapout_" + Integer.toString(i));
            BufferedReader input = new BufferedReader(new InputStreamReader(inputStream));
            while ((line = input.readLine()) != null) {
                String[] words = line.split(" ");
                if (!wordsSumMap.containsKey(words[0])) {
                    wordsSumMap.put(words[0], 1);
                }
                else {
                    wordsSumMap.put(words[0], wordsSumMap.get(words[0]) + 1);
                }
            }
            inputStream.close();
            input.close();
        }

        // ----------------Write output file----------------
        System.out.println("Writing result.");
        FSDataOutputStream reduceOut = fs.create(outputPath);
        for (Map.Entry<String, Integer> entry : wordsSumMap.entrySet()) {
            reduceOut.writeBytes(entry.getKey() + " " + entry.getValue().toString() + "\n");
        }

        //  ----------------Test----------------
        long endTime = System.currentTimeMillis();
        System.out.println("Container runtime: " + (endTime - startTime) + "ms");
        //  ----------------Test----------------
    }
}
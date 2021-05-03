package galaxy.testfile;

import java.io.FileWriter;
import java.io.IOException;

import com.google.common.collect.Maps;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import java.util.Map;
import java.util.HashMap;

public class TestInteger{
    public static void main(String[] args) throws Exception {
        Map<String, Integer> wordsSumMap = new HashMap<String, Integer>();
        if (wordsSumMap.containsKey("abc")) {
            wordsSumMap.put("abc", 1);
        }
        
        wordsSumMap.put("abc", wordsSumMap.get("abc")+111111);
        Integer sum = wordsSumMap.get("abc");
        System.out.println(sum.toString());
    }
}

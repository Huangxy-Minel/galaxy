/**
    Description: Class for executor
    Author: Minel Huang
    Date: 2021/05/03
 */

package galaxy.executor;

import galaxy.store.job.Job;
import java.lang.ProcessBuilder;
import java.lang.Process;
import java.lang.Thread;
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class Executor {
    public Job job;
    public Process process;
    public String cmd;
    public BufferedReader reader;
    public boolean exitFlag = false;        // True means process has been completed

    public void executor () throws Exception {
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command(cmd.split("\\s+"));
        process = processBuilder.start();
        reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        // Thread.currentThread().sleep(100);
        String line;
        String[] words;
        while((line = reader.readLine()) != null) {
            if (line.indexOf("Submitting application") != -1) {
                words = line.split(" ");
                job.jobId = words[2];
                break;
            }
        }
        System.out.println("Start job: " + job.jobId);
    }

    public void readExecutor () throws Exception {
        String line;
        String[] words;
        while ((line = reader.readLine()) != null) {
            if (line.indexOf("Client runtime") != -1) {
                words = line.split(" ");
                job.completeTime = words[2];
            }
            if (line.indexOf("finished with state FINISHED") != -1) {
                job.status = "finished";
                exitFlag = true;
            }
        }
    }
}
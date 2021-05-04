/**
    Description: Class for executor
    Author: Minel Huang
    Date: 2021/05/03
 */

package galaxy.executor;

import galaxy.store.job.Job;
import java.lang.ProcessBuilder;
import java.lang.Process;

public class Executor {
    public Job job;
    public Process process;

    public void executor () throws Exception {
        // String cmd = "yarn jar /home/galaxy/galaxy.jar " + job.clientPath;
        // process = Runtime.getRuntime().exec(cmd);
    }
}
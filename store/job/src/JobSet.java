/**
    Description: Job set, stores Job class
    Author: Minel Huang
    Date: 2021/05/03
 */


package galaxy.store.job;

import galaxy.store.job.Job;
import java.util.HashSet;

public class JobSet {
    public HashSet<Job> set = new HashSet<Job>();
    /**
        Function: Add element to set
        Input Para: Job
        Output Para: None
     */
    public void addJob(Job job) throws Exception {
        set.add(job);
    }

    /**
        Function: Del element to set
        Input Para: Job
        Output Para: None
     */
    public void delJob(Job job) throws Exception {
        set.remove(job);
    }
}
/**
    Description: Job queue, stores Job class
    Author: Minel Huang
    Date: 2021/05/03
 */


package galaxy.store.job;

import galaxy.store.job.Job;
import java.util.ArrayList;

public class JobList {
    public ArrayList<Job> list = new ArrayList<Job>();

    /**
        Function: Add element to queue
        Input Para: Job
        Output Para: None
     */
    public void addJob(Job job) throws Exception {
        list.add(job);
    }

    /**
        Function: Del element to queue
        Input Para: Job
        Output Para: None
     */
    public void delJob(Job job) throws Exception {
        list.remove(job);
    }
}
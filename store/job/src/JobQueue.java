/**
    Description: Job queue, stores Job class
    Author: Minel Huang
    Date: 2021/05/03
 */


package galaxy.store.job;

import galaxy.store.job.Job;
import java.util.LinkedList;
import java.util.Queue;

public class JobQueue {
    public static Queue<Job> queue = new LinkedList<Job>();

    /**
        Function: Add element to queue
        Input Para: Job
        Output Para: None
     */
    public static void addJob(Job job) throws Exception {
        queue.offer(job);
    }

    /**
        Function: Del element to queue
        Input Para: None
        Output Para: Job
     */
    public static void delJob(Job job) throws Exception {
        queue.remove(job);
    }
}
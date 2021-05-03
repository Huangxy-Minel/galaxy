/**
    Description: Class for storing definitions and methods of Job
    Author: Minel Huang
    Date: 2021/05/03
 */


package galaxy.store.job;

import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import galaxy.store.container.Container;

public class Job {
    public static String jobName = "default";
    public static String jobId = "";
    public static String jobType = "mapreduce";
    public static String clientPath = "";
    public static Integer priority = 0;
    public static String status = "waiting";
    public static Container allocateContainer;

    /**
        Function: Create default Job class.
        Input Para: None
        Output Para: Job
     */
    public static void initJob(Job job) throws Exception {
        job.jobName = "default";
        Date date=new Date();
        DateFormat dateFormat=new SimpleDateFormat("yyyyMMdd");
        job.jobId = dateFormat.format(date) + "_0000";
        job.jobType = "mapreduce";
        job.clientPath = "";
        job.priority = 0;
        job.status = "waiting";
    }
}
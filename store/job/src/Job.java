/**
    Description: Class for storing definitions and methods of Job
    Author: Minel Huang
    Date: 2021/05/03
 */


package galaxy.store.job;

import java.util.Date;
import java.util.ArrayList;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import galaxy.store.container.Container;

public class Job {
    public String jobName = "default";
    public String jobId = "";
    public String jobType = "mapreduce";
    public String clientPath = "";
    public Integer priority = 0;
    public String status = "waiting";
    public ArrayList<Container> allocateContainer = new ArrayList<Container>();

    /**
        Function: Create default Job class.
        Input Para: None
        Output Para: Job
     */
    public void initJob(Job job) throws Exception {
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
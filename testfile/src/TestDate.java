package galaxy.testfile;

import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import galaxy.store.job.Job;
import java.util.ArrayList;

public class TestDate {
    public static void main(String[] args) throws Exception {
        ArrayList<Job> workingJobList = new ArrayList<Job>();
        ArrayList<Job> waitingJobList = new ArrayList<Job>();
        Job testJob = new Job();
        testJob.status = "waiting";
        workingJobList.add(testJob);
        waitingJobList.add(testJob);
        for (Job job : workingJobList) {
            job.status = "working";
        }
        System.out.println(waitingJobList.get(0).status);
    }
}
/**
    Description: Class of Allocator
    Author: Minel Huang
    Date: 2021/05/11
 */

package galaxy.scheduler.scheduler_model;

import galaxy.store.job.*;
import galaxy.store.container.*;
import java.util.ArrayList;
import java.util.Iterator;

public class SchedulerModel {

    /**
    Function: Scheduler for developing cpu utilization
    Input Para: 
        jobList: List of Jobs, Type: JobList
    */
    public static void CPUUtilizationScheduler (JobList jobList) {
        for (Job job : jobList.list) {
            if (job.status == "waiting" && job.priority == 0) {    // get waiting LC jobs

            }
        }
    }

    /**
    Function: Scheduler for LC jobs
    Input Para: 
        jobList: List of Jobs, Type: JobList
    */
    public static void LCScheduler (JobList jobList) {
        ArrayList<Job> runningJobList = new ArrayList<Job>();
        ArrayList<Job> waitingLCJobList = new ArrayList<Job>();
        for (Job job : jobList.list) {
            
        }
    }

    /**
    Function: Default. Execute LC jobs immediately.
    Input Para: 
        jobList: List of Jobs, Type: JobList
    */
    public static void DefaultScheduler (JobList jobList, JobSet runningJobs, int currentRound, ContainerPool containerPool) {
        int waitingLCJobNum = 0;
        ArrayList<Job> LCList = new ArrayList<Job>();

        //--------------------------------Get LC jobs--------------------------------
        Iterator<Job> it_jobList = jobList.list.iterator();
        while (it_jobList.hasNext()) {
            Job job = it_jobList.next();
            if (job.status == "waiting" && job.jobType == "LC") {
                if (currentRound - job.enterRound > job.requireCompleteRound - 10) {
                    waitingLCJobNum++;
                    LCList.add(job);
                    it_jobList.remove();
                }
            }
        }

        //--------------------------------Modify jobList--------------------------------
        int idx = 0;
        for (; idx < jobList.list.size(); idx++) {
            if (jobList.list.get(idx).status == "waiting") {
                break;
            }
        }
        for (Job lcJob : LCList) {
            jobList.list.add(idx, lcJob);
            idx++;
        }

        //--------------------------------Modify jobList--------------------------------
        for (Job job : runningJobs.set) {
            if (job.jobType != "LC" && waitingLCJobNum > 0) {
                job.changeFlag = true;
                containerPool.freeMemory += job.vMemory;
                containerPool.freeVCores += job.vCores;
                waitingLCJobNum--;
            }
        }

    }

}
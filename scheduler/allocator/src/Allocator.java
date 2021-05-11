/**
    Description: Class of Allocator
    Author: Minel Huang
    Date: 2021/05/11
 */

package galaxy.scheduler.allocator;

import galaxy.store.job.*;
import galaxy.store.container.*;

public class Allocator {

    /**
    Function: Default Allocator
    Input Para: 
        jobList: List of Jobs, Type: JobList
    Output Para:
        nextRoundJobs: Type: JobSet
    */
    public static JobSet DefaultAllocator (JobList jobList, ContainerPool containerPool) throws Exception {
        JobSet nextRoundJobs = new JobSet();
        for (Job job : jobList.list) {
            if (job.status == "waiting" && containerPool.addContainerfromJob(job)) {
                nextRoundJobs.addJob(job);
            }
        }
        return nextRoundJobs;
    }
}
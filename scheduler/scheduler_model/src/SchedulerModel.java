/**
    Description: Class of Allocator
    Author: Minel Huang
    Date: 2021/05/11
 */

package galaxy.scheduler.shceduler_model;

public class SchedulerModel {

    /**
    Function: Scheduler for developing cpu utilization
    Input Para: 
        jobList: List of Jobs, Type: JobList
    */
    public void CPUUtilizationScheduler (JobList jobList) {
        for (Job job : jobList.list) {
            if (job.status == "waiting" && job.priority == 0) {    // get waiting LC jobs

            }
        }
    }

    /**
    Function: Scheduler for capacity
    Input Para: 
        jobList: List of Jobs, Type: JobList
    */
    public void CapacityScheduler (JobList jobList) {

    }


}
/**
    Description: Galaxy server
    Author: Minel Huang
    Date: 2021/05/03
 */

package galaxy.galaxy_server;

import galaxy.store.job.*;
import galaxy.store.container.*;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class GalaxyServer {

    public static void main(String[] args) throws Exception {
        // ----------------Init----------------
        int maxJobNum = 2;
        JobQueue jobQueue = new JobQueue();
        JobSet runningJobs = new JobSet();
        ContainerPool containerPool = new ContainerPool();

        Integer round = 0;

        // ----------------Generate random jobs----------------
        Date date = new Date();
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        for (int i = 0; i < maxJobNum; i++) {
            Job newJob = new Job();
            newJob.jobName = "test_" + Integer.toString(i);
            newJob.jobId = dateFormat.format(date) + "_" + Integer.toString(i);
            newJob.clientPath = "galaxy.yarnapp.hello.Client";
            jobQueue.addJob(newJob);
        }

        // ----------------Server starts----------------
        while (!jobQueue.queue.isEmpty()){
            System.out.println("Current Round: " + round.toString());

            // ----------------GalaxyScheduler-RM----------------
            JobSet nextRoundJobs = new JobSet();
            for (Job job : jobQueue.queue) {
                if (job.status == "waiting") {
                    Container container = new Container();
                    job.allocateContainer = container;
                    nextRoundJobs.addJob(job);
                }
            }

            // ----------------Change job status----------------
            for (Job job : runningJobs.set) {
                if (job.status == "finished") {
                    containerPool.delContainer(job.allocateContainer);
                    runningJobs.delJob(job);
                    jobQueue.delJob(job);
                }
            }

            // ----------------Excuter----------------
            for (Job job : nextRoundJobs.set) {
                if (containerPool.addContainer(job.allocateContainer)) {
                    job.status = "working";
                    runningJobs.addJob(job);
                }
            }
            // ----------------Debug----------------
            System.out.println("freeMemory: " + Integer.toString(containerPool.freeMemory));
            System.out.println("freeVCores: " + Integer.toString(containerPool.freeVCores));
            System.out.println("working container num: " + Integer.toString(containerPool.workingPool.size()));
            System.out.println("running jobs num: " + Integer.toString(runningJobs.set.size()));
            System.out.println("job queue size: " + Integer.toString(jobQueue.queue.size()));

            for (Job job : runningJobs.set) {
                job.status = "finished";
            }
            round++;
        }
    }
}
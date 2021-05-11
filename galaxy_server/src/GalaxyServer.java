/**
    Description: Galaxy server
    Author: Minel Huang
    Date: 2021/05/03
 */

package galaxy.galaxy_server;

import galaxy.executor.Executor;
import galaxy.store.job.*;
import galaxy.store.container.*;

import java.lang.ProcessBuilder;
import java.lang.Process;
import java.lang.Thread;
import java.util.Date;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class GalaxyServer {

    public static void main(String[] args) throws Exception {
        // ----------------Init----------------
        int maxJobNum = 1;
        JobList jobList = new JobList();
        JobSet runningJobs = new JobSet();
        ContainerPool containerPool = new ContainerPool();
        ArrayList<Executor> executorList = new ArrayList<Executor>();

        Integer round = 0;

        // ----------------Generate random jobs----------------
        randomJob(maxJobNum, jobList);

        // ----------------Server starts----------------
        while (!jobList.list.isEmpty()){
            System.out.println("----------------Current Round: " + round.toString() + "----------------");

            // ----------------GalaxyScheduler-RM----------------
            JobSet nextRoundJobs = new JobSet();
            for (Job job : jobList.list) {
                if (job.status == "waiting") {
                    job.allocateContainer.add(new Container());    // add working containers
                    Container AMContainer = new Container();
                    AMContainer.vMemory = 128;
                    job.allocateContainer.add(AMContainer);        // add AM
                    job.vCores += 1;
                    job.vMemory += 64 + 128;
                    nextRoundJobs.addJob(job);
                }
            }

            // ----------------Change job status----------------
            Iterator<Job> it_runningJobs = runningJobs.set.iterator();
            while (it_runningJobs.hasNext()) {
                Job job = it_runningJobs.next();
                if (job.status == "finished") {
                    System.out.println(job.jobName + " has completed, job id: " + job.jobId);
                    containerPool.delContainer(job.allocateContainer);
                    jobList.delJob(job);
                    it_runningJobs.remove();
                }
            }

            // ----------------Excuter----------------
            for (Job job : nextRoundJobs.set) {
                if (containerPool.addContainer(job.allocateContainer)) {
                    job.status = "working";
                    runningJobs.addJob(job);
                    Executor newExecutor = new Executor();
                    ProcessBuilder processBuilder = new ProcessBuilder();
                    newExecutor.job = job;
                    String cmd = "yarn jar /home/galaxy/galaxy.jar " + job.clientPath + " " + Integer.toString(job.vCores) + " " + 
                                                                       Integer.toString(job.vMemory) + " " + Integer.toString(job.priority) + 
                                                                       job.fileDir;
                    processBuilder.command(cmd.split("\\s+"));
                    newExecutor.process = processBuilder.start();
                    executorList.add(newExecutor);
                    Thread.currentThread().sleep(1000);
                }
            }

            // ----------------Debug----------------
            System.out.println("freeMemory: " + Integer.toString(containerPool.freeMemory));
            System.out.println("freeVCores: " + Integer.toString(containerPool.freeVCores));
            System.out.println("working container num: " + Integer.toString(containerPool.workingPool.size()));
            System.out.println("running jobs num: " + Integer.toString(runningJobs.set.size()));
            System.out.println("Executor num: " + Integer.toString(executorList.size()));
            System.out.println("job list size: " + Integer.toString(jobList.list.size()));

            // ----------------Check process status----------------
            Iterator<Executor> it_executorList = executorList.iterator();
            while (it_executorList.hasNext()) {
                Executor executor = it_executorList.next();
                executor.job.status = "finished";
                it_executorList.remove();
            }
            round++;
            Thread.currentThread().sleep(5000);
        }
    }

    /**
        Function: Generate Job randomly
        Input Para:
            maxJobNum
            jobList
        Output Para:
            jobList
    */
    public static void randomJob (int maxJobNum, JobList jobList) {
        // ----------------Init----------------
        Random random = new Random();
        String[] clientPath = {"galaxy.yarnapp.hello.Client", "galaxy.yarnapp.mapreduce.Client", "galaxy.yarnapp.machine.Client"};   // Three types of jobs
        String[] fileDir = {"", "/user/galaxy/mrinput", "/user/galaxy/mlinput"};
        String[] jobName = {"latency-critical", "word-count", "logistic-regression"};
        Date date = new Date();
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

        // ----------------Generate----------------
        for (i = 0, i < maxJobNum, i++) {
            Job newJob = new Job();
            int jobType = random.nextInt(clientPath.size());
            newJob.jobName = jobName[jobType] + "_" + Integer.toString(i);
            newJob.jobId = dateFormat.format(date) + "_" + Integer.toString(i);
            newJob.clientPath = clientPath[jobType];
            newJob.fileDir = fileDir[jobType];
            newJob.priority = jobType;
            newJob.order = i;
            jobList.addJob(newJob);
        }
    }
}
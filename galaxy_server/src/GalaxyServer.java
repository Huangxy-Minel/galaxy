/**
    Description: Galaxy server
    Author: Minel Huang
    Date: 2021/05/03
 */

package galaxy.galaxy_server;

import galaxy.executor.Executor;
import galaxy.store.job.*;
import galaxy.store.container.*;
import galaxy.scheduler.allocator.Allocator;
import galaxy.scheduler.scheduler_model.SchedulerModel;

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
        int maxJobNum = 15;
        JobList jobList = new JobList();
        JobSet runningJobs = new JobSet();
        ContainerPool containerPool = new ContainerPool();
        ArrayList<Executor> executorList = new ArrayList<Executor>();

        // ----------------Debug----------------
        ArrayList<Integer> jobRuntimeList = new ArrayList<Integer>();
        int overtimeLCNum = 0;
        int killJobNum = 0;

        Integer round = 0;

        // ----------------Generate random jobs----------------
        randomJob(maxJobNum, jobList, round, 0);
        int jobIdx = maxJobNum;

        // ----------------Server starts----------------
        while (!jobList.list.isEmpty()){

            System.out.println("------------------------Current Round: " + round.toString() + "------------------------");
            
            // ----------------Check process status----------------
            Iterator<Executor> it_executorList = executorList.iterator();
            while (it_executorList.hasNext()) {
                Executor executor = it_executorList.next();
                if (!executor.process.isAlive()) {
                    executor.readExecutor();
                    executor.reader.close();
                    it_executorList.remove();
                }
            }

            // ----------------Change job status----------------
            Iterator<Job> it_runningJobs = runningJobs.set.iterator();
            while (it_runningJobs.hasNext()) {
                Job job = it_runningJobs.next();
                if (job.status == "finished") {
                    System.out.println(job.jobName + " has completed, job id: " + job.jobId);
                    System.out.println("Job runtime: " + job.completeTime + "ms");
                    jobRuntimeList.add(Integer.parseInt(job.completeTime));
                    containerPool.delContainerfromJob(job);
                    jobList.delJob(job);
                    if (job.jobType == "LC" && round - job.enterRound > job.requireCompleteRound) {
                        overtimeLCNum++;
                    }
                    it_runningJobs.remove();
                }
            }

            // ----------------GalaxyScheduler-RM----------------
            SchedulerModel.DefaultScheduler(jobList, runningJobs, round, containerPool);
            JobSet nextRoundJobs = Allocator.DefaultAllocator(jobList, containerPool);

            // ----------------Excuter----------------
            killJobNum += execute(nextRoundJobs, executorList, runningJobs);

            // ----------------Debug----------------
            System.out.println("freeMemory: " + Integer.toString(containerPool.freeMemory));
            System.out.println("freeVCores: " + Integer.toString(containerPool.freeVCores));
            System.out.println("running jobs num: " + Integer.toString(runningJobs.set.size()));
            // System.out.println("----------------running jobs list:----------------");
            // for (Job job : runningJobs.set) {
            //     System.out.print(job.jobName + "  ");
            // }
            // System.out.print("\n");
            System.out.println("Executor num: " + Integer.toString(executorList.size()));
            System.out.println("Job list size: " + Integer.toString(jobList.list.size()));
            System.out.println("Overtime LC jobs num: " + Integer.toString(overtimeLCNum));
            System.out.println("Killed jobs num: " + Integer.toString(killJobNum));
            // System.out.println("----------------jobs list:----------------");
            // for (Job job : jobList.list) {
            //     System.out.print(job.jobName + "  ");
            // }
            // System.out.print("\n");

            round++;
            randomJob(2, jobList, round, jobIdx);
            jobIdx += 2;
            Thread.currentThread().sleep(2000);
        }
        double averageRuntime = arrAverage(jobRuntimeList);
        System.out.println("----------------All jobs complete----------------");
        System.out.println("Mean of job runtime: " + averageRuntime);
    }

    /**
        Function: Generate Job randomly
        Input Para:
            maxJobNum
            jobList
        Output Para:
            jobList
    */
    public static void randomJob (int maxJobNum, JobList jobList, Integer round, int jobIdx) throws Exception {
        // ----------------Init----------------
        Random random = new Random();
        int typeNum = 3;
        String[] clientPath = {"galaxy.yarnapp.hello.Client", "galaxy.yarnapp.mapreduce.Client", "galaxy.yarnapp.machine_learning.Client"};   // Three types of jobs
        String[] fileDir = {"/", "/user/galaxy/mrinput/", "/user/galaxy/mlinput/"};
        String[] jobName = {"latency-critical", "word-count", "logistic-regression"};

        // ----------------Generate----------------
        for (int i = 0; i < maxJobNum; i++) {
            Job newJob = new Job();
            int jobType = random.nextInt(typeNum - 1);
            // jobType = 1;
            newJob.jobName = jobName[jobType] + "_" + Integer.toString(jobIdx + i);
            newJob.clientPath = clientPath[jobType];
            newJob.fileDir = fileDir[jobType];
            newJob.priority = jobType;
            newJob.order = i;
            newJob.enterRound = round;
            // Add resource requirements of users
            switch (jobType) {
                case 0: newJob.vCores = 2; newJob.vMemory = 128; newJob.jobType = "LC"; break;
                case 1: newJob.vCores = 3; newJob.vMemory = 320; newJob.jobType = "MR"; break;
                case 2: newJob.vCores = 11; newJob.vMemory = 1344; newJob.jobType = "ML"; break;
            }
            jobList.addJob(newJob);
        }
    }

    public static int execute (JobSet nextRoundJobs, ArrayList<Executor> executorList, JobSet runningJobs) throws Exception {
        int killJobNum = 0;
        // Step 1: kill all jobs whose status have changed
        Iterator<Job> it_runningJobs = runningJobs.set.iterator();
        while (it_runningJobs.hasNext()) {
            Job job = it_runningJobs.next();
            if (job.changeFlag) {
                job.changeFlag = false;
                job.status = "waiting";
                ProcessBuilder processBuilder = new ProcessBuilder();
                String cmd = "yarn app -kill " + job.jobId;
                processBuilder.command(cmd.split("\\s+"));
                Process process = processBuilder.start();
                Thread.currentThread().sleep(100);
                it_runningJobs.remove();
                System.out.println("Kill job: " + job.jobId);
                killJobNum++;
            }
        }
        for (Job job : nextRoundJobs.set) {
            job.status = "working";
            runningJobs.addJob(job);
            Executor newExecutor = new Executor();
            newExecutor.job = job;
            newExecutor.cmd = "yarn jar /home/galaxy/galaxy.jar " + job.clientPath + " " + job.jobName + " " + Integer.toString(job.vCores) + " " + 
                                                                Integer.toString(job.vMemory) + " " + Integer.toString(job.priority) 
                                                                + " " + job.fileDir;
            newExecutor.executor();
            executorList.add(newExecutor);
        }
        return killJobNum;
    }

    public static double arrAverage (ArrayList<Integer> arr) {
        double sum = 0;
        for (Integer i : arr) {
            sum += i.intValue();
        }
        return sum / arr.size();
    }
}
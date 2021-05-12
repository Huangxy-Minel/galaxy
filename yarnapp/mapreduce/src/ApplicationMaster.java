package galaxy.yarnapp.mapreduce;

import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ClassUtil;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import galaxy.store.container.ContainerPool;
import galaxy.store.flow.Flow;

import java.io.File;
import java.util.Map;
import java.lang.Integer;
import java.util.ArrayList;


public class ApplicationMaster {
    /**
        Input Para: 
            args[0]: VCores num
            args[1]: vMemory num
            args[2]: priority
            args[3]: Input file dir
     */
    public static void main(String[] args) throws Exception {
        try {
            // ----------------Init----------------
            YarnConfiguration conf = new YarnConfiguration();
            FileSystem fs = FileSystem.get(conf);
            ContainerPool containerPool = new ContainerPool();
            containerPool.freeVCores = Integer.valueOf(args[0]).intValue() - 1;
            containerPool.freeMemory = Integer.valueOf(args[1]).intValue() - 64;
            int jobPriority = Integer.valueOf(args[2]).intValue();
            Path fileDir = new Path(args[3]);
            ArrayList<Flow> flowList = new ArrayList<Flow>();

            // ----------------Init Flow----------------
            ContentSummary content = fs.getContentSummary(fileDir);
            long fileCount = content.getFileCount();
            for (int i = 0; i < fileCount; i++) {
                Flow flow = new Flow();
                flow.flowPath = args[3] + "random_text" + Integer.toString(i);
                flowList.add(flow);
            }

            // ----------------Create clients to talk to RM & NM----------------
            // Client for RM
            AMRMClient<ContainerRequest> rmClient = AMRMClient.createAMRMClient();
            rmClient.init(conf);
            rmClient.start();
            //Client for NM
            NMClient nmClient = NMClient.createNMClient();
            nmClient.init(conf);
            nmClient.start();
            // Register
            System.out.println("registerApplicationMaster: pending");
            rmClient.registerApplicationMaster("", 0, "");
            System.out.println("registerApplicationMaster: complete");

            // ----------------Init record of contianer----------------
            Priority priority = Records.newRecord(Priority.class);
            priority.setPriority(jobPriority);
            Resource capability = Records.newRecord(Resource.class);

            // ----------------Start----------------

            // ----------------GalaxyScheduler-AM----------------
            for (Flow flow : flowList) {
                flow.vCores = containerPool.freeVCores / flowList.size();
                flow.vMemory = containerPool.freeMemory / flowList.size();
                // Config requirements of containers
                capability.setVirtualCores(flow.vCores);
                capability.setMemory(flow.vMemory);
                // Make container requests to ResourceManager
                ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
                System.out.println("adding container ask:" + containerAsk);
                rmClient.addContainerRequest(containerAsk);
                TimeUnit.SECONDS.sleep(1);
            }

            // ----------------Begin Map----------------
            System.out.println("----------------Begin Map----------------");
            // ----------------Wait and launch containers----------------
            int allocatedContainer = 0;
            String cmd = "/home/galaxy/hadoop-3.2.2/bin/hadoop jar Container.jar galaxy.dataprocess.mapreduce.wordcount.Mapper /user/galaxy/mroutput/";
            while (allocatedContainer < flowList.size()) {
                System.out.println("Waiting for containers......");
                AllocateResponse response = rmClient.allocate(0);
                for (Container container : response.getAllocatedContainers()) {
                    ContainerId containerID = container.getId();
                    System.out.println("Get a container! ID: " + containerID.toString());
                    ContainerLaunchContext ctx = createContainerLaunchContext(conf, fs, cmd, flowList.get(allocatedContainer).flowPath, allocatedContainer, "random_text");
                    System.out.println("Launching container " + container);
                    nmClient.startContainer(container, ctx);
                    allocatedContainer++;
                }
                TimeUnit.SECONDS.sleep(1);
            }

            // ----------------Wait for containers to complete----------------
            int completedContainer = 0;
            while (completedContainer < flowList.size()) {
                System.out.println("allocate (wait)");
                AllocateResponse response = rmClient.allocate(0);
                for (ContainerStatus status : response.getCompletedContainersStatuses()) {
                    System.out.println("Completed container " + status);
                    completedContainer++;
                }
                TimeUnit.SECONDS.sleep(1);
            }
            System.out.println("----------------End Map----------------");

            // ----------------Begin Reduce----------------
            System.out.println("----------------Begin Reduce----------------");
            capability.setVirtualCores(containerPool.freeVCores);
            capability.setMemory(containerPool.freeMemory);
            ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
            System.out.println("adding container ask:" + containerAsk);
            rmClient.addContainerRequest(containerAsk);

            // ----------------Wait and launch containers----------------
            allocatedContainer = 0;
            String cmd = "/home/galaxy/hadoop-3.2.2/bin/hadoop jar Container.jar galaxy.dataprocess.mapreduce.wordcount.Reducer /user/galaxy/mroutput/";
            while (allocatedContainer < 1) {
                System.out.println("Waiting for containers......");
                AllocateResponse response = rmClient.allocate(0);
                for (Container container : response.getAllocatedContainers()) {
                    ContainerId containerID = container.getId();
                    System.out.println("Get a container! ID: " + containerID.toString());
                    ContainerLaunchContext ctx = createContainerLaunchContext(conf, fs, cmd, flowList.get(allocatedContainer).flowPath, allocatedContainer, "random_text");
                    System.out.println("Launching container " + container);
                    nmClient.startContainer(container, ctx);
                    allocatedContainer++;
                }
                TimeUnit.SECONDS.sleep(1);
            }

            // ----------------Wait for containers to complete----------------
            int completedContainer = 0;
            while (completedContainer < flowList.size()) {
                System.out.println("allocate (wait)");
                AllocateResponse response = rmClient.allocate(0);
                for (ContainerStatus status : response.getCompletedContainersStatuses()) {
                    System.out.println("Completed container " + status);
                    completedContainer++;
                }
                TimeUnit.SECONDS.sleep(1);
            }
            System.out.println("----------------End Redice----------------");

            // ----------------Un-register with ResourceManager----------------
            System.out.println("unregister");
            rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
            System.out.println("exiting");
        }
        catch(Throwable t) {
            t.printStackTrace();
        }
    }
    public static ContainerLaunchContext createContainerLaunchContext (YarnConfiguration conf, FileSystem fs, String cmd, String flowPath, int idx, String fileName) throws Exception {
        ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
        // Set launch commands
        // final String cmd = "/home/galaxy/hadoop-3.2.2/bin/hadoop jar Container.jar galaxy.dataprocess.mapreduce.wordcount.Mapper /user/galaxy/lcoutput/";
        // final String cmd = "/home/galaxy/hadoop-3.2.2/bin/hadoop jar Container.jar galaxy.dataprocess.mapreduce.wordcount.Reducer /user/galaxy/lcoutput/";
        String ctnLaunchCmd =
            String.format(
                "%s 1>%s/stdout 2>%s/stderr",
                cmd + " " + String.valueOf(idx), 
                ApplicationConstants.LOG_DIR_EXPANSION_VAR,
                ApplicationConstants.LOG_DIR_EXPANSION_VAR);
        ctx.setCommands(Lists.newArrayList(ctnLaunchCmd));
        // Add source code for Container
        Path jarPath = new Path(fs.getHomeDirectory(), "galaxy.jar");
        LocalResource containerJar = addResourse(jarPath, conf, fs);
        // Add input file for Container
        // Path filePath = new Path(fs.getHomeDirectory(), "lcoutput/mapout_0");
        Path filePath = new Path(flowPath);
        LocalResource inputFile = addResourse(filePath, conf, fs);
        ctx.setLocalResources(
            ImmutableMap.of("Container.jar", containerJar,
                            fileName, inputFile));
        // Set classpath for Client
        Map<String, String> ctxEnv = Maps.newHashMap();
        for (String c : conf.getStrings(
            YarnConfiguration.YARN_APPLICATION_CLASSPATH,
            YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
        Apps.addToEnvironment(ctxEnv, Environment.CLASSPATH.name(), c.trim());
        }
        Apps.addToEnvironment(ctxEnv,
            Environment.CLASSPATH.name(),
            Environment.PWD.$() + File.separator + "*");
        ctx.setEnvironment(ctxEnv);

        return ctx;
    }

    public static LocalResource addResourse (Path filePath, YarnConfiguration conf, FileSystem fs) throws Exception {
        LocalResource file = Records.newRecord(LocalResource.class);
        FileStatus fileStat = FileSystem.get(conf).getFileStatus(filePath);
        file.setResource(ConverterUtils.getYarnUrlFromPath(filePath));
        file.setSize(fileStat.getLen());
        file.setTimestamp(fileStat.getModificationTime());
        file.setType(LocalResourceType.FILE);
        file.setVisibility(LocalResourceVisibility.APPLICATION);
        return file;
    }
}
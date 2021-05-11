// Function: Apply containers to run galaxy.testfile.Hello
package galaxy.yarnapp.hello;

import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
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

import java.io.File;
import java.util.Map;


public class ApplicationMaster {
    public static void main(String[] args) throws Exception {
        try {
            System.out.println(args[0]);
            // ----------------Create clients to talk to RM & NM----------------
            YarnConfiguration conf = new YarnConfiguration();
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

            // ----------------Ask for container----------------
            // Config requirements of containers
            Priority priority = Records.newRecord(Priority.class);
            priority.setPriority(0);
            Resource capability = Records.newRecord(Resource.class);
            capability.setMemory(64);
            capability.setVirtualCores(1);
            // Make container requests to ResourceManager
            ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
            System.out.println("adding two container asks:" + containerAsk);
            rmClient.addContainerRequest(containerAsk);

            // ----------------Wait and launch containers----------------
            int allocatedContainer = 0;
            while (allocatedContainer < 1) {
                System.out.println("Waiting for containers......");
                AllocateResponse response = rmClient.allocate(0);
                for (Container container : response.getAllocatedContainers()) {
                    ContainerId containerID = container.getId();
                    System.out.println("Get a container! ID: " + containerID.toString());
                    allocatedContainer++;
                    ContainerLaunchContext ctx = createContainerLaunchContext(conf);
                    System.out.println("Launching container " + container);
                    nmClient.startContainer(container, ctx);
                }
                TimeUnit.SECONDS.sleep(1);
            }
            
            //  ----------------Test----------------
            long startTime = System.currentTimeMillis();
            //  ----------------Test----------------

            // ----------------Wait for containers to complete----------------
            boolean completedContainer = false;
            while (!completedContainer) {
                System.out.println("allocate (wait)");
                AllocateResponse response = rmClient.allocate(0);
                for (ContainerStatus status : response.getCompletedContainersStatuses()) {
                    completedContainer = true;
                    System.out.println("Completed container " + status);
                }
                TimeUnit.SECONDS.sleep(1);
            }

            //  ----------------Test----------------
            long endTime = System.currentTimeMillis();
            System.out.println("Container deploy plus run time: " + (endTime - startTime) + "ms");
            //  ----------------Test----------------

            // ----------------Un-register with ResourceManager----------------
            System.out.println("unregister");
            rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
            System.out.println("exiting");
        }
        catch(Throwable t) {
            t.printStackTrace();
        }
    }
    public static ContainerLaunchContext createContainerLaunchContext(YarnConfiguration conf) throws Exception {
        ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
        // Set launch commands
        final String cmd = "/home/galaxy/hadoop-3.2.2/bin/hadoop jar Container.jar galaxy.testfile.Hello";
        String ctnLaunchCmd =
            String.format(
                "%s 1>%s/stdout 2>%s/stderr",
                cmd, 
                ApplicationConstants.LOG_DIR_EXPANSION_VAR,
                ApplicationConstants.LOG_DIR_EXPANSION_VAR);
        ctx.setCommands(Lists.newArrayList(ctnLaunchCmd));
        // Add resource for Container
        LocalResource containerJar = Records.newRecord(LocalResource.class);
        FileSystem fs = FileSystem.get(conf);
        Path jarPath = new Path(fs.getHomeDirectory(), "galaxy.jar");
        FileStatus jarStat = FileSystem.get(conf).getFileStatus(jarPath);
        containerJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
        containerJar.setSize(jarStat.getLen());
        containerJar.setTimestamp(jarStat.getModificationTime());
        containerJar.setType(LocalResourceType.FILE);
        containerJar.setVisibility(LocalResourceVisibility.APPLICATION);
        ctx.setLocalResources(
            ImmutableMap.of("Container.jar", containerJar));

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
}
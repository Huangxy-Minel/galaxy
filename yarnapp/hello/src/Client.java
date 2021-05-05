// Function: Create and Monitor ApplicationMaster
// Author: MinelHuang
// Date: 2021/04/22
package galaxy.yarnapp.hello;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ClassUtil;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.File;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Client {
    /**
        Input Para: 
            args[0]: Job name
            args[1]: VCores num
            args[2]: Memory num
     */
    public static void main(String[] args) throws Exception {
        System.out.println("Galaxy version 1.0");
        System.out.println("Author: MinelHuang");
        System.out.println("UESTC & HUKST");

        // ----------------Create Client----------------
        YarnConfiguration conf = new YarnConfiguration();
        YarnClient yarnClient = YarnClient.createYarnClient();      // create new instance
        yarnClient.init(conf);
        yarnClient.start();

        // ----------------copy jar to HDFS----------------
        String jar = ClassUtil.findContainingJar(Client.class);
        FileSystem fs = FileSystem.get(conf);
        Path src = new Path(jar);
        Path dest = new Path(fs.getHomeDirectory(), src.getName());
        System.out.format("Copying JAR from %s to %s%n", src, dest);
        fs.copyFromLocalFile(src, dest);

        // ----------------Config AM----------------
        ApplicationSubmissionContext appContext = createAM(yarnClient, conf, dest, args[0]);

        // ----------------Submit AM to RM----------------
        ApplicationId appId = appContext.getApplicationId();
        System.out.println("Submitting application " + appId);
        yarnClient.submitApplication(appContext);

        // ----------------Monitor AM state----------------
        ApplicationReport report = yarnClient.getApplicationReport(appId);
        YarnApplicationState state = report.getYarnApplicationState();
        EnumSet terminalStates =
            EnumSet.of(YarnApplicationState.FINISHED,
                YarnApplicationState.KILLED,
                YarnApplicationState.FAILED);
        // Wait for app to complete
        while (!terminalStates.contains(state)) {
        TimeUnit.SECONDS.sleep(1);
        report = yarnClient.getApplicationReport(appId);
        state = report.getYarnApplicationState();
        }

        System.out.printf("Application %s finished with state %s%n",
            appId, state);
    }

    public static ApplicationSubmissionContext createAM(YarnClient yarnClient, YarnConfiguration conf, Path jarURL, String jobName) throws Exception {
        System.out.println("create and configure AM");
        // Apply container for AM
        YarnClientApplication app = yarnClient.createApplication();

        // ----------------Config Submission Context----------------
        ContainerLaunchContext container = Records.newRecord(ContainerLaunchContext.class);
        // Add launch Cmd
        String amLaunchCmd =
            String.format(
                "$JAVA_HOME/bin/java -Xmx128M %s 1>%s/stdout 2>%s/stderr",
                ApplicationMaster.class.getName() + " hello",
                ApplicationConstants.LOG_DIR_EXPANSION_VAR,
                ApplicationConstants.LOG_DIR_EXPANSION_VAR);
        container.setCommands(Lists.newArrayList(amLaunchCmd));
        // Add local resource for AM
        LocalResource appMasterJar = Records.newRecord(LocalResource.class);
        FileStatus jarStat = FileSystem.get(conf).getFileStatus(jarURL);
        appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarURL));
        appMasterJar.setSize(jarStat.getLen());
        appMasterJar.setTimestamp(jarStat.getModificationTime());
        appMasterJar.setType(LocalResourceType.FILE);
        appMasterJar.setVisibility(LocalResourceVisibility.APPLICATION);
        container.setLocalResources(
            ImmutableMap.of("AppMaster.jar", appMasterJar));
        // Set classpath for AM
        Map<String, String> appMasterEnv = Maps.newHashMap();
        for (String c : conf.getStrings(
            YarnConfiguration.YARN_APPLICATION_CLASSPATH,
            YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
        Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), c.trim());
        }
        Apps.addToEnvironment(appMasterEnv,
            Environment.CLASSPATH.name(),
            Environment.PWD.$() + File.separator + "*");
        container.setEnvironment(appMasterEnv);

        // ----------------Config resource requirements for AM----------------
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(128);
        capability.setVirtualCores(1);
        // Set up ApplicationSubmissionContext
        ApplicationSubmissionContext appContext =
            app.getApplicationSubmissionContext();
        appContext.setApplicationName(jobName); // application name
        appContext.setAMContainerSpec(container);
        appContext.setResource(capability);
        appContext.setQueue("default"); // queue

        return appContext;
    }
}
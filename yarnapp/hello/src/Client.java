// Function: print Hello World
// Author: MinelHuang
// Date: 2021/04/22
package galaxy.yarnapp.hello;

import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.util.Records;

import java.io.File;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Client {
    public static void main(String[] args) throws Exception {
        System.out.println("Galaxy version 1.0");
        System.out.println("Author: MinelHuang");
        System.out.println("UESTC & HUKST");

        // Create Client
        YarnConfiguration conf = new YarnConfiguration();
        YarnClient yarnClient = YarnClient.createYarnClient();      // create new instance
        yarnClient.init(conf);
        // yarnClient.start();
        // Config AM
        YarnClientApplication app = createAM(yarnClient);
        // Submit AM to RM
        // yarnClient.submitApplication(appContext);
    }

    public static YarnClientApplication createAM(YarnClient yarnClient) throws Exception {
        System.out.println("create and configure AM");
        // Apply container for AM
        YarnClientApplication app = yarnClient.createApplication();
        // Config Submission Context
        ContainerLaunchContext container = Records.newRecord(ContainerLaunchContext.class);
        // Add launch Cmd
        // String amLaunchCmd =
        //     String.format(
        //         "$JAVA_HOME/bin/java -Xmx256M %s 1>%s/stdout 2>%s/stderr",
        //         ApplicationMaster.class.getName(),
        //         ApplicationConstants.LOG_DIR_EXPANSION_VAR,
        //         ApplicationConstants.LOG_DIR_EXPANSION_VAR);
        // container.setCommands(Lists.newArrayList(amLaunchCmd));
        return app;
    }
}
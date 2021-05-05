/**
    Description: Class for storing definitions and methods of Flow
    Author: Minel Huang
    Date: 2021/05/05
 */


package galaxy.store.flow;

import java.util.ArrayList;

import galaxy.store.container.Container;

public class Flow {
    public int fileSize = 0;        // MB
    public String flowPath = "";
    public String executorPath = "";
    public Integer priority = 0;
    public String status = "waiting";
    public ArrayList<Container> allocateContainer = new ArrayList<Container>();
    public int vCores = 0;
    public int vMemory = 0;
}
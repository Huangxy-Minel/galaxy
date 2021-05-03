/**
    Description: Container set, stores Container class
    Author: Minel Huang
    Date: 2021/05/03
 */


package galaxy.store.container;

import galaxy.store.container.Container;
import java.util.HashSet;

public class ContainerPool {
    public static int totalMemory = 6144;
    public static int totalVCores = 16;
    public static HashSet<Container> workingPool = new HashSet<Container>();
    public static int freeMemory = 6144;
    public static int freeVCores = 16;

    /**
        Function: Add container to working pool
        Input Para: Container
        Output Para: None
     */
    public static boolean addContainer(Container container) throws Exception {
        if (freeMemory > container.vMemory && freeVCores > container.vCores) {
            workingPool.add(container);
            freeMemory -= container.vMemory;
            freeVCores -= container.vCores;
        }
        else {
            return false;
        }
        return true;
    }

    /**
        Function: Del container in working pool
        Input Para: Container
        Output Para: true: success
     */
    public static boolean delContainer(Container container) throws Exception {
        if (workingPool.contains(container)) {
            freeMemory += container.vMemory;
            freeVCores += container.vCores;
            workingPool.remove(container);
        }
        else {
            return false;
        }
        return true;
    }
}
/**
    Description: Container set, stores Container class
    Author: Minel Huang
    Date: 2021/05/03
 */


package galaxy.store.container;

import galaxy.store.container.Container;
import java.util.HashSet;
import java.util.ArrayList;

public class ContainerPool {
    public static int totalMemory = 6144;
    public static int totalVCores = 16;
    public HashSet<Container> workingPool = new HashSet<Container>();
    public int freeMemory = 6144;
    public int freeVCores = 16;

    /**
        Function: Add container to working pool
        Input Para: Container
        Output Para: None
     */
    public boolean addContainer(ArrayList<Container> containerList) throws Exception {
        int reservedMemory = 0;
        int reservedCores = 0;
        for (Container container : containerList) {
            reservedMemory += container.vMemory;
            reservedCores += container.vCores;
        }
        if (freeMemory > reservedMemory && freeVCores > reservedCores) {
            for (Container container : containerList) {
                workingPool.add(container);
            }
            freeMemory -= reservedMemory;
            freeVCores -= reservedCores;
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
    public boolean delContainer(ArrayList<Container> containerList) throws Exception {
        for (Container container : containerList) {
            if (workingPool.contains(container)) {
                freeMemory += container.vMemory;
                freeVCores += container.vCores;
                workingPool.remove(container);
            }
            else {
                return false;
            }
        }
        return true;
    }
}
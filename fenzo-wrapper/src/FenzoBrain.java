import com.netflix.fenzo.*;
import org.apache.mesos.Protos;

import java.util.*;

public class FenzoBrain {
    public static void main(String[] args) {
        TaskScheduler taskScheduler = new TaskScheduler.Builder()
                //todo: add more stuff
                .build();
        final Map<String, String> launchedTasks = new HashMap<>();

        //main loop
        try {
            while (true) {
                System.out.println("FenzoBrain: collecting resource offers from Layer-X @ ip ");
                List<Protos.Offer> newOffers = getNewResourceOffers();
                System.out.println("FenzoBrain: collecting pending tasks from Layer-X @ ip ");
                List<Protos.TaskInfo> newTasks = getTasks();
                System.out.println("FenzoBrain: scheduling " + newTasks.size() + " tasks across " + newOffers.size() + " offers...");
                Map<String, Protos.TaskInfo> taskInfoMap = new HashMap<>();
                List<TaskRequest> taskRequests = new ArrayList<>();
                for (Protos.TaskInfo taskInfo : newTasks){
                    TaskRequest taskRequest = FenzoInfo.fromTaskInfo(taskInfo);
                    taskInfoMap.put(taskRequest.getId(), taskInfo);
                    taskRequests.add(taskRequest);
                }
                List<VirtualMachineLease> vmLeases = new ArrayList<>();
                for (Protos.Offer offer : newOffers){
                    VirtualMachineLease vmLease = FenzoInfo.fromOffer(offer);
                    vmLeases.add(vmLease);
                }
                SchedulingResult schedulingResult = taskScheduler.scheduleOnce(taskRequests, vmLeases);
                System.out.println("result=" + schedulingResult);
                Map<String,VMAssignmentResult> resultMap = schedulingResult.getResultMap();
                if(!resultMap.isEmpty()) {
                    for(VMAssignmentResult vmAssignmentResult: resultMap.values()) {
                        List<VirtualMachineLease> leasesUsed = vmAssignmentResult.getLeasesUsed();
                        StringBuilder stringBuilder = new StringBuilder("Sending to Layer-X: " + leasesUsed.get(0).hostname() + " tasks ");
                        for (VirtualMachineLease lease : leasesUsed){
                            for (TaskAssignmentResult taskAssignmentResult : vmAssignmentResult.getTasksAssigned()){
                                stringBuilder.append(taskAssignmentResult.getTaskId()).append(", ");
                                Protos.TaskInfo taskToLaunch = taskInfoMap.get(taskAssignmentResult.getTaskId());
                                // remove task from pending tasks map and put into launched tasks map
                                // (in real world, transition the task state)
                                launchedTasks.put(taskAssignmentResult.getTaskId(), lease.hostname());
                                taskScheduler.getTaskAssigner().call(taskAssignmentResult.getRequest(), lease.hostname());
                                System.out.println(stringBuilder.toString());
                                scheduleTaskOnLayerX(taskToLaunch, lease.getOffer());
                            }
                        }
                    }
                }

                Thread.sleep(3000);
            }
        }catch (InterruptedException e) {
            System.out.println("Shutting down!");
        }

    }

    private static List<Protos.Offer> getNewResourceOffers() {
        List<Protos.Offer> offers = new ArrayList<>();

        return offers;
    }

    private static List<Protos.TaskInfo> getTasks() {
        List<Protos.TaskInfo> tasks = new ArrayList<>();
        return tasks;
    }

    private static void scheduleTaskOnLayerX(Protos.TaskInfo task, Protos.Offer offer){

    }

}

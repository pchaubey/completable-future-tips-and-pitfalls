package com.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TimeoutHandlingPitfall {
    private static final Logger log = LoggerFactory.getLogger(TimeoutHandlingPitfall.class);
    private static ExecutorService executor = Executors.newCachedThreadPool();
    public static void main(String[] args) {

        var result = runTask(1);
        var result1 = runTask(2);
        var result2 = runTask(3);

        var res1 = result.join();
        var res2 = result1.join();
        var res3 = result2.join();
    }

    private static CompletableFuture<Integer> runTask(int taskId) {
        return processAsync(taskId).orTimeout(5, TimeUnit.SECONDS)
                .handle((t, ex) -> {
                            if (ex != null) {
                                log.info("{} timed out in thread {} at {}, starting followup task to handle timeout", getTaskName(taskId), Thread.currentThread().getName(), new Date());
                                return processFollowupAfterTimeoutAsync(taskId).join();
                            } else {
                                //log.info("{} successfully executed in thread {} at {}", getTaskName(taskId), Thread.currentThread().getName(), new Date());
                                return t;
                            }
                        }
                ).whenComplete((t, ex) -> {
                    log.info("{} final result {}", getTaskName(taskId), t);
                });
    }

    private static CompletableFuture<Integer> processAsync(int taskId) { // should ideally return a negative value
        return CompletableFuture.supplyAsync(() -> {
            //log.info("start executing task: {} in thread {} ", getTaskName(taskId), Thread.currentThread().getName());
            try {
                Thread.sleep(10000);
                return taskId;
            } catch (InterruptedException e) {
                //log.info("{} interrupted {}", getTaskName(taskId), Thread.currentThread().getName());
                e.printStackTrace();
                return -1;
            }
        }, executor);
    }

    private static CompletableFuture<Integer> processFollowupAfterTimeoutAsync(int taskId) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                //log.info("Executing followup task {} after timeout in thread {} ", getTaskName(taskId), Thread.currentThread().getName());
                Thread.sleep(3000);
                return -1 * taskId;
            } catch (InterruptedException e) {
                //log.info("{} interrupted {}", getTaskName(taskId), Thread.currentThread().getName());
                e.printStackTrace();
                return -1;
            }
        }, executor);
    }

    private static String getTaskName(int id) {
        return "task-" + id;
    }
}

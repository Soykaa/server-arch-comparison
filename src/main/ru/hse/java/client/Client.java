package ru.hse.java.client;

import ru.hse.java.common.InformationKeeper;
import main.ru.hse.java.proto.Query;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class Client implements Runnable {
    private final Socket socket;
    private final DataInputStream input;
    private final DataOutputStream output;
    private final ScheduledExecutorService sendToServer = newSingleThreadScheduledExecutor();
    private final ExecutorService readFromServer = newSingleThreadExecutor();
    private static ConcurrentHashMap<Integer, ClientTask> tasks;

    public Client(String host, int port) throws IOException {
        InetAddress address;
        address = InetAddress.getByName(host);
        socket = new Socket(address, port);
        input = new DataInputStream(socket.getInputStream());
        output = new DataOutputStream(socket.getOutputStream());
        tasks = new ConcurrentHashMap<>();
    }

    private static class ClientTask {
        private long start;
        private long finish;
        private ArrayList<Integer> sortedData;

        ClientTask() {}

        public void sortAndSetData(ArrayList<Integer> data) {
            Collections.sort(data);
            sortedData = data;
        }
    }

    @Override
    public void run() {
        new ScheduledTask(sendToServer, output).scheduleTask();
        readFromServer.submit(() -> {
            try {
                int acceptedTasks = 0;
                while (acceptedTasks != InformationKeeper.getNumberOfQueries()) {
                    int messageSize = input.readInt();
                    byte [] message = new byte[messageSize];
                    input.readFully(message);
                    Query allMessage = Query.parseFrom(message);
                    int taskId = allMessage.getId();
                    List<Integer> sortedData = allMessage.getNumList();
                    if (sortedData.equals(tasks.get(taskId).sortedData)) {
                        long finish = System.currentTimeMillis();
                        ClientTask curTask = tasks.get(taskId);
                        curTask.finish = finish;
                        tasks.replace(taskId, curTask);
                        acceptedTasks++;
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        try {
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static class ScheduledTask implements Runnable {
        private final ScheduledExecutorService threadPool;
        private final DataOutputStream output;
        private final Random rand = new Random();

        public ScheduledTask(ScheduledExecutorService threadPool, DataOutputStream output) {
            this.threadPool = threadPool;
            this.output = output;
        }

        @Override
        public void run() {
            /* generating data */
            ArrayList<Integer> list = new ArrayList<>();
            for (int i = 0; i < InformationKeeper.getArraySize(); i++) {
                int number = rand.nextInt();
                list.add(number);
            }
            ClientTask curTask = new ClientTask();
            curTask.sortAndSetData(list);
            int taskId = tasks.size();
            curTask.start = System.currentTimeMillis();
            tasks.put(taskId, curTask);
            Query query = Query.newBuilder().setId(taskId).setSize(list.size()).addAllNum(list).build();
            try {
                output.writeInt(query.toByteArray().length);
                output.write(query.toByteArray());
                output.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
            tasks.put(taskId, curTask);
            scheduleTask();
        }

        public void scheduleTask() {
            threadPool.schedule(this, InformationKeeper.getDelta(), TimeUnit.SECONDS);
        }
    }
}
package ru.hse.java.server;

import ru.hse.java.utils.BubbleSort;
import ru.hse.java.common.InformationKeeper;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import main.ru.hse.java.proto.Query;

public class BlockingArch implements Server {
    private final ExecutorService serverSocketService = Executors.newSingleThreadExecutor();
    private final ExecutorService workerThreadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() - 2);

    private volatile boolean isWorking = true;
    private ServerSocket serverSocket;

    private final ConcurrentHashMap.KeySetView<ClientData, Boolean> clients = ConcurrentHashMap.newKeySet();
    @Override
    public void start() {
        try {
            serverSocket = new ServerSocket(InformationKeeper.getPort());
        } catch (IOException e) {
            e.printStackTrace();
        }
        serverSocketService.submit(() -> acceptClients(serverSocket));
    }

    @Override
    public void stop() {
        isWorking = false;
        try {
            serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        workerThreadPool.shutdownNow();
        serverSocketService.shutdownNow();
        clients.forEach(ClientData::close);

    }

    private void acceptClients(ServerSocket serverSocket) {
        try (ServerSocket ignored = serverSocket) {
            while (isWorking) {
                try {
                    Socket socket = serverSocket.accept();
                    ClientData clientData = new ClientData(socket);
                    clients.add(clientData);
                    clientData.processClient();
                } catch (IOException e) {
                    stop();
                }
            }
            stop();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private class ClientData {
        private final Socket socket;
        public final ExecutorService responseWriter = Executors.newSingleThreadExecutor();
        public final ExecutorService requestReader = Executors.newSingleThreadExecutor();

        private final DataInputStream inputStream;
        private final DataOutputStream outputStream;

        private volatile boolean working = true;
        private final ConcurrentHashMap<Integer, ClientTask> timestamps;

        private ClientData(Socket socket) throws IOException {
            this.socket = socket;
            timestamps = new ConcurrentHashMap<>();
            inputStream = new DataInputStream(socket.getInputStream());
            outputStream = new DataOutputStream(socket.getOutputStream());
        }

        private class ClientTask {
            private long start;
            private long finish;
            private ArrayList<Integer> sortedData;
            private int index;
        }

        public void sendResponse(ClientTask curTask) {
            responseWriter.submit(() -> {
                try {
                    long finish = System.currentTimeMillis();
                    Query query = Query.newBuilder().setId(curTask.index)
                            .setSize(curTask.sortedData.size()).addAllNum(curTask.sortedData).build();
                    outputStream.writeInt(query.toByteArray().length);
                    outputStream.write(query.toByteArray().length);
                    outputStream.flush();
                    curTask.finish = finish;
                    timestamps.replace(curTask.index, curTask);
                } catch (IOException e) {
                    close();
                }
            });
        }

        public void processClient() {
            requestReader.submit(() -> {
                try {
                    while(working) {
                        int messageSize = inputStream.readInt();
                        byte [] message = new byte[messageSize];
                        inputStream.readFully(message);

                        Query allMessage = Query.parseFrom(message);
                        int index = allMessage.getId();
                        List<Integer> data = allMessage.getNumList();
                        int [] arrayToSort = data.stream().mapToInt(i->i).toArray();
                        ClientTask curTask = new ClientTask();
                        curTask.start = System.currentTimeMillis();
                        curTask.index = index;
                        timestamps.put(index, curTask);
                        workerThreadPool.submit(() -> {
                            BubbleSort.sort(arrayToSort);
                            sendResponse(curTask);
                        });
                    }
                } catch (IOException e) {
                    close();
                }
            });
        }

        public void close() {
            working = false;
            responseWriter.shutdownNow();
            requestReader.shutdownNow();
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

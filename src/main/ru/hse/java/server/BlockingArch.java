package ru.hse.java.server;

import ru.hse.java.utils.BubbleSort;
import ru.hse.java.common.InformationKeeper;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
        private final ConcurrentHashMap<Integer, TimeStartFinish> timestamps;

        private ClientData(Socket socket) throws IOException {
            this.socket = socket;
            timestamps = new ConcurrentHashMap<>();
            inputStream = new DataInputStream(socket.getInputStream());
            outputStream = new DataOutputStream(socket.getOutputStream());
        }

        private class TimeStartFinish {
            private long start;
            private long finish;
        }

        public void sendResponse(int [] data) {
            responseWriter.submit(() -> {
                try {
                    long finish = System.currentTimeMillis();
//                    TODO: ответ от сервера
                    TimeStartFinish curTSF = timestamps.get(index);
                    curTSF.finish = finish;
                    timestamps.replace(index, curTSF);
                } catch (IOException e) {
                    close();
                }
            });
        }

        public void processClient() {
            requestReader.submit(() -> {
                try {
                    while(working) {
                        int [] data;
//                        TODO: чтение массива
                        TimeStartFinish curTSF = new TimeStartFinish();
                        curTSF.start = System.currentTimeMillis();
                        timestamps.put(index, curTSF);
                        workerThreadPool.submit(() -> {
                            BubbleSort.sort(data);
                            sendResponse(data);
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

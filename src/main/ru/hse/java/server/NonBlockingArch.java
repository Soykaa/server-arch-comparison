package ru.hse.java.server;

import ru.hse.java.common.InformationKeeper;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NonBlockingArch implements Server {
    private final ExecutorService serverSocketService = Executors.newSingleThreadExecutor();
    private final ExecutorService workerThreadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() - 2);
    private final ConcurrentHashMap.KeySetView<ClientData, Boolean> clients = ConcurrentHashMap.newKeySet();
    private volatile boolean isWorking = true;
    private ServerSocket serverSocket;

    private final ExecutorService requestThreadPool = Executors.newSingleThreadExecutor();
    private final RequestHandler requestHandler = new RequestHandler();
    private final ExecutorService responseThreadPool = Executors.newSingleThreadExecutor();
    private final ResponseHandler responseHandler = new ResponseHandler();

    @Override
    public void start() {
        try {
            serverSocket = new ServerSocket(InformationKeeper.getPort());
        } catch (IOException e) {
            e.printStackTrace();
        }
        requestThreadPool.submit(requestHandler);
        responseThreadPool.submit(responseHandler);
        serverSocketService.submit(() -> acceptClients(serverSocket));
    }

    private void acceptClients(ServerSocket serverSocket) {
        try (ServerSocket ignored = serverSocket) {
            while (isWorking) {
                try {
                    Socket socket = serverSocket.accept();
                    ClientData clientData = new ClientData(socket.getChannel());
                    clients.add(clientData);
                    requestHandler.registrationQueueRequest.add(clientData);
                    requestHandler.requestSelector.wakeup();
                } catch (IOException e) {
                    stop();
                }
            }
            stop();
        } catch (IOException e) {
            e.printStackTrace();
        }
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
    }

    private class RequestHandler implements Runnable {
        private Selector requestSelector;
        private final ConcurrentLinkedQueue<ClientData> registrationQueueRequest = new ConcurrentLinkedQueue<>();

        RequestHandler() {
            try {
                requestSelector = Selector.open();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            while (isWorking) {
                try {
                    requestSelector.select();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                Set<SelectionKey> selectedKeys = requestSelector.selectedKeys();
                Iterator<SelectionKey> iterator = selectedKeys.iterator();
                while (iterator.hasNext()) {
                    SelectionKey key = iterator.next();
                    if (key.isReadable()) {
                        ClientData clientData = (ClientData) key.attachment();
                        try {
                            clientData.read();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    iterator.remove();
                }
                registerClientsInSelector();
            }
        }

        private void registerClientsInSelector() {
            while (!registrationQueueRequest.isEmpty()) {
                ClientData client = registrationQueueRequest.poll();
                SocketChannel socketChannel = client.getSocketChannel();
                try {
                    socketChannel.configureBlocking(false);
                    socketChannel.register(requestSelector, SelectionKey.OP_READ, client);
                } catch (IOException e) {
                    stop();
                }
            }
        }
    }

    private class ResponseHandler implements Runnable {
        private Selector responseSelector;
        private final ConcurrentLinkedQueue<ClientData> registrationQueueResponse = new ConcurrentLinkedQueue<>();

        ResponseHandler() {
            try {
                responseSelector = Selector.open();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            while (isWorking) {
                try {
                    responseSelector.select();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                Set<SelectionKey> selectedKeys = responseSelector.selectedKeys();
                Iterator<SelectionKey> iterator = selectedKeys.iterator();
                while (iterator.hasNext()) {
                    SelectionKey key = iterator.next();
                    if (key.isWritable()) {
                        ClientData clientData = (ClientData) key.attachment();
                        try {
                            clientData.write(key);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    iterator.remove();
                }
                registerClientsInSelector();
            }
        }

        private void registerClientsInSelector() {
            while (!registrationQueueResponse.isEmpty()) {
                ClientData client = registrationQueueResponse.poll();
                SocketChannel socketChannel = client.getSocketChannel();
                try {
                    socketChannel.configureBlocking(false);
                    socketChannel.register(responseSelector, SelectionKey.OP_WRITE, client);
                } catch (IOException e) {
                    stop();
                }
            }
        }
    }

    private class ClientData {
        private final SocketChannel socketChannel;
        private final ByteBuffer readBuffer = ByteBuffer.allocate(1000000);
        private final ConcurrentLinkedQueue<ByteBuffer> tasksToWrite = new ConcurrentLinkedQueue<>();

        private boolean isReadSize;
        private boolean isReadIndex;
        private byte [] array;
        private int numbersCounter;
        private boolean newTask = true;

        private final ConcurrentHashMap<Integer, ClientTask> timestamps;

        private class ClientTask {
            private long start;
            private long finish;
            private int size;
            private int index;
        }

        public ClientData(SocketChannel socketChannel) {
            timestamps = new ConcurrentHashMap<>();
            this.socketChannel = socketChannel;
            try {
                socketChannel.configureBlocking(false);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public void read() throws IOException {
            ClientTask curTask;
            if (newTask) {
                long start = System.currentTimeMillis();
                newTask = false;
                curTask = new ClientTask();
                curTask.start = start;
            }
            /* non blocking read */
            socketChannel.read(readBuffer);
            readBuffer.flip();

            /* read up to the end from buffer */
            if (!isReadSize && readBuffer.remaining() >= 8) {
                size = readBuffer.getInt();
                isReadSize = true;
            }
            if (!isReadIndex && readBuffer.remaining() >= 4) {
                index = readBuffer.getInt();
                if (start != -1) {
                    ClientTask curTask = new ClientTask();
                    curTask.start = start;
                    curTask.index = index;
                    curTask.size = size;
                    timestamps.put(index, curTSF);
                }
                isReadIndex = true;
                array = new byte[size - 4];
            }
            while (readBuffer.remaining() > 0 && isReadSize) {
                array[numbersCounter++] = readBuffer.get();
            }
            readBuffer.compact();

            /* write data */
            if (numbersCounter == array.length && isReadSize && isReadIndex) {
                byte [] data = array;
                isReadSize = false;
                isReadIndex = false;
                numbersCounter = 0;
                // TODO protobuf --> index, size
                /* sorting an array */
                workerThreadPool.submit(() -> {
                    List<Integer> sortedList = IntStream.of(BubbleSort(arrayToSort)).boxed().collect(Collectors.toList()));
                    long end = System.currentTimeMillis();
                    TimeStartFinish curTSF = timestamps.get(index);
                    timestamps.replace(index, curTSF);
                    // TODO protobuf
                    ByteBuffer writeBuffer = ByteBuffer.allocate(10000000);
                    /* write to buffer */
                    writeBuffer.putInt(size);
                    writeBuffer.putInt(index);
                    writeBuffer.put(data.toByteArray());
                    writeBuffer.flip();
                    tasksToWrite.add(writeBuffer);
                    responseHandler.registrationQueueResponse.add(this);
                    responseHandler.responseSelector.wakeup();
                });
            }
            readBuffer.compact();
        }

        public void write(SelectionKey key) throws IOException {
            ByteBuffer writeBuffer = tasksToWrite.peek();
            if (writeBuffer != null) {
                socketChannel.write(writeBuffer);
                if (writeBuffer.remaining() == 0) {
                    tasksToWrite.remove();
                    newTask = true;
                }
            }
            if (tasksToWrite.isEmpty()) {
                key.cancel();
            }
        }

        public SocketChannel getSocketChannel() {
            return socketChannel;
        }
    }
}
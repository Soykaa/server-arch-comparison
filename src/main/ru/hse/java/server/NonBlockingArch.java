package ru.hse.java.server;

import ru.hse.java.common.InformationKeeper;

import java.io.IOException;
import java.net.InetSocketAddress;
import main.ru.hse.java.proto.Query;
import ru.hse.java.utils.BubbleSort;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class NonBlockingArch implements Server {
    private final ExecutorService serverSocketService = Executors.newSingleThreadExecutor();
    private final ExecutorService workerThreadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() - 2);
    private final ConcurrentHashMap.KeySetView<ClientData, Boolean> clients = ConcurrentHashMap.newKeySet();
    private volatile boolean isWorking = true;
    private ServerSocketChannel serverSocketChannel;

    private final ExecutorService requestThreadPool = Executors.newSingleThreadExecutor();
    private final RequestHandler requestHandler = new RequestHandler();
    private final ExecutorService responseThreadPool = Executors.newSingleThreadExecutor();
    private final ResponseHandler responseHandler = new ResponseHandler();

    @Override
    public void start() {
        try {
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.socket().bind(new InetSocketAddress(InformationKeeper.getPort()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        requestThreadPool.submit(requestHandler);
        responseThreadPool.submit(responseHandler);
        serverSocketService.submit(() -> acceptClients(serverSocketChannel));
    }

    private void acceptClients(ServerSocketChannel serverSocketChannel) {
        try (ServerSocketChannel ignored = serverSocketChannel) {
            while (isWorking) {
                try {
                    SocketChannel socketChannel = serverSocketChannel.accept();
                    socketChannel.configureBlocking(false);
                    ClientData clientData = new ClientData(socketChannel);
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
            serverSocketChannel.close();
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
                    socketChannel.register(responseSelector, SelectionKey.OP_WRITE, client);
                } catch (IOException e) {
                    stop();
                }
            }
        }
    }

    private class ClientData {
        private final SocketChannel socketChannel;
        private ByteBuffer messageReadBuffer;
        private ByteBuffer messageSizeReadBuffer;
        private final ConcurrentLinkedQueue<ByteBuffer> tasksToWrite = new ConcurrentLinkedQueue<>();

        private boolean isReadSize = false;
        private boolean newTask = true;

        private ClientTask curTask;
        private int size;

        private final ConcurrentHashMap<Integer, ClientTask> timestamps;

        private class ClientTask {
            private long start;
            private long finish;
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
            if (newTask) {
                long start = System.currentTimeMillis();
                newTask = false;
                curTask = new ClientTask();
                curTask.start = start;
                messageSizeReadBuffer = ByteBuffer.allocate(4);
            }

            if (!isReadSize) {
                socketChannel.read(messageSizeReadBuffer);
                messageSizeReadBuffer.flip();
                if (messageSizeReadBuffer.remaining() == 0) {
                    isReadSize = true;
                    size = messageSizeReadBuffer.getInt();
                    messageReadBuffer = ByteBuffer.allocate(size);
                }
                messageSizeReadBuffer.flip();
            } else {
                socketChannel.read(messageReadBuffer);
                messageReadBuffer.flip();
                if (messageReadBuffer.remaining() == 0) {
                    byte [] message = new byte[size];
                    messageReadBuffer.get(message);
                    Query allMessage = Query.parseFrom(message);
                    int index = allMessage.getId();
                    List<Integer> data = allMessage.getNumList();
                    int [] arrayToSort = data.stream().mapToInt(i->i).toArray();
                    curTask.start = System.currentTimeMillis();
                    timestamps.put(index, curTask);
                    newTask = true;
                    isReadSize = false;
                    workerThreadPool.submit(() -> {
                        BubbleSort.sort(arrayToSort);
                        curTask.finish = System.currentTimeMillis();
                        List<Integer> sortedData = IntStream.of(arrayToSort).boxed()
                                .collect(Collectors.toList());
                        ByteBuffer writeBuffer = ByteBuffer.allocate(4 + size);
                        Query query = Query.newBuilder().setId(index)
                                .setSize(sortedData.size()).addAllNum(sortedData).build();
                        writeBuffer.putInt(query.toByteArray().length);
                        writeBuffer.put(query.toByteArray());
                        writeBuffer.flip();
                        tasksToWrite.add(writeBuffer);
                        responseHandler.registrationQueueResponse.add(this);
                        responseHandler.responseSelector.wakeup();
                    });
                }
                messageReadBuffer.flip();
            }
        }

        public void write(SelectionKey key) throws IOException {
            ByteBuffer writeBuffer = tasksToWrite.peek();
            if (writeBuffer != null) {
                socketChannel.write(writeBuffer);
                if (writeBuffer.hasRemaining()) {
                    tasksToWrite.remove();
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
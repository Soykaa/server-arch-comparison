package ru.hse.java.common;

public class InformationKeeper {
    /* штуки из условия */
    private static long arraySize; // количество элементов в сортируемых массивах
    private static long numOfClients; //  количество одновременно работающих клиентов
    private static long delta; // временной промежуток <...>
    private static long numberOfQueries; //  суммарное количество запросов, отправляемых каждым клиентом
    private static final int port = 4444;

    /* задает пользователь */
    private static ServerArchType serverArchType;
    private static Parameter parameter;
    private static long lowBorder;
    private static long highBorder;
    private static long step;
    private static Metrics curMetrics;

    public enum ServerArchType {
        BLOCKING,
        NON_BLOCKING,
        ASYNCHRONOUS
    }

    public enum Parameter {
        ARRAY_SIZE,
        CLIENTS_NUMBER,
        DELTA
    }
    private enum Metrics {
        CLIENT_TIME,
        AVERAGE_QUERY_TIME
    }

    public static long getArraySize() {
        return arraySize;
    }

    public static void setArraySize(long arraySize) {
        InformationKeeper.arraySize = arraySize;
    }

    public static long getNumOfClients() {
        return numOfClients;
    }

    public static void setNumOfClients(long numOfClients) {
        InformationKeeper.numOfClients = numOfClients;
    }

    public static long getDelta() {
        return delta;
    }

    public static void setDelta(long delta) {
        InformationKeeper.delta = delta;
    }

    public static long getNumberOfQueries() {
        return numberOfQueries;
    }

    public static void setNumberOfQueries(long numberOfQueries) {
        InformationKeeper.numberOfQueries = numberOfQueries;
    }

    public static ServerArchType getServerArchType() {
        return serverArchType;
    }

    public static void setServerArchType(ServerArchType serverArchType) {
        InformationKeeper.serverArchType = serverArchType;
    }

    public static Parameter getChangingParameter() {
        return parameter;
    }

    public static void setChangingParameter(String changingParameter) {
        if (changingParameter.equals("n")) {
            parameter = Parameter.ARRAY_SIZE;
        } else if (changingParameter.equals("m")) {
            parameter = Parameter.CLIENTS_NUMBER;
        } else {
            parameter = Parameter.DELTA;
        }
    }

    public static long getLowBorder() {
        return lowBorder;
    }

    public static void setLowBorder(long lowBorder) {
        InformationKeeper.lowBorder = lowBorder;
    }

    public static long getHighBorder() {
        return highBorder;
    }

    public static void setHighBorder(long highBorder) {
        InformationKeeper.highBorder = highBorder;
    }

    public static long getStep() {
        return step;
    }

    public static void setStep(long step) {
        InformationKeeper.step = step;
    }

    public static Metrics getCurMetrics() {
        return curMetrics;
    }

    public static void setCurMetrics(Metrics curMetrics) {
        InformationKeeper.curMetrics = curMetrics;
    }

    public static int getPort() {
        return port;
    }

    public static Parameter getParameter() {
        return parameter;
    }

    public static void setChangingParameter(long i) {
        switch (getParameter()) {
            case ARRAY_SIZE:
                setArraySize(i);
                break;
            case CLIENTS_NUMBER:
                setNumOfClients(i);
                break;
            case DELTA:
                setDelta(i);
                break;
        }
    }
}

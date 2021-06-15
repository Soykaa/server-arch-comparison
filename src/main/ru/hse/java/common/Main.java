package ru.hse.java.common;

import ru.hse.java.client.Client;
import ru.hse.java.server.BlockingArch;
import ru.hse.java.server.NonBlockingArch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        Scanner in = new Scanner(System.in);
        System.out.print("Input a server architecture type: \n" +
                "** 0 - Blocking, 1 - NonBlocking, 2 - Asynchronous **\n");
        int serverArchTypeNum = in.nextInt();
        if (serverArchTypeNum < 0 || serverArchTypeNum >= InformationKeeper.ServerArchType.values().length) {
            System.err.println("Unknown architecture type");
        }
        InformationKeeper.ServerArchType serverArchType = InformationKeeper.ServerArchType.values()[serverArchTypeNum];
        InformationKeeper.setServerArchType(serverArchType);

        System.out.print("Input a number of queries:\n");
        InformationKeeper.setNumberOfQueries(in.nextLong());

        System.out.print("Choose a parameter, that will change:\n" +
                "** n - array size, m - number of clients, d - delta **\n");
        InformationKeeper.setChangingParameter(in.next());

        System.out.print("Input a low border:\n");
        InformationKeeper.setLowBorder(in.nextLong());

        System.out.print("Input a high border:\n");
        InformationKeeper.setHighBorder(in.nextLong());

        System.out.print("Input a step:\n");
        InformationKeeper.setStep(in.nextLong());

        if (InformationKeeper.getChangingParameter() == InformationKeeper.Parameter.ARRAY_SIZE) {
            System.out.print("Input a number of clients:\n");
            InformationKeeper.setNumOfClients(in.nextLong());
            System.out.print("Input a delta:\n");
            InformationKeeper.setDelta(in.nextLong());
            InformationKeeper.setArraySize(in.nextLong());
        } else if (InformationKeeper.getChangingParameter() == InformationKeeper.Parameter.CLIENTS_NUMBER) {
            System.out.print("Input a array size:\n");
            InformationKeeper.setArraySize(in.nextLong());
            System.out.print("Input a delta:\n");
            InformationKeeper.setDelta(in.nextLong());
            InformationKeeper.setNumOfClients(in.nextLong());
        } else {
            System.out.print("Input a array size:\n");
            InformationKeeper.setArraySize(in.nextLong());
            System.out.print("Input a number of clients:\n");
            InformationKeeper.setNumOfClients(in.nextInt());
            InformationKeeper.setDelta(in.nextLong());
        }

        for (long i = InformationKeeper.getLowBorder();
             i <= InformationKeeper.getHighBorder(); i += InformationKeeper.getStep()) {
            InformationKeeper.setChangingParameter(i);

            List<Client> clientList = new ArrayList<>((int) InformationKeeper.getNumOfClients());

            for (int j = 0; j < InformationKeeper.getNumOfClients(); j++) {
                try {
                    clientList.add(new Client("localhost", InformationKeeper.getPort()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (InformationKeeper.getServerArchType() == InformationKeeper.ServerArchType.BLOCKING) {
                new BlockingArch().start();
            } else if (InformationKeeper.getServerArchType() == InformationKeeper.ServerArchType.NON_BLOCKING) {
                new NonBlockingArch().start();
            } else {
                throw new UnsupportedOperationException();
            }

            for (int j = 0; j < InformationKeeper.getNumOfClients(); j++) {
                clientList.get(j).run();
            }
        }
        //+ TODO statistics
    }
}

package ru.hse.java.utils;

public class BubbleSort {
    public static void sort(int [] array) {
        long arraySize = array.length;

        for (int i = 0; i < arraySize; i++) {
            for (int j = 0; j < arraySize - 1; j++) {
                if (array[j] > array[j + 1]) {
                    int t = array[j];
                    array[j] = array[j + 1];
                    array[j + 1] = t;
                }
            }
        }
    }
}

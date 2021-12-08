package zutsoft.util;

import java.io.File;
import java.io.FileWriter;
import java.util.Random;

/**
 * 随机生成输入文件
 */
public class RandomFileGeneration {

    public static void generateGroupedNums() {
        try {
            String[] a = { "a", "b", "c", "d" };
            Random rand = new Random();
            String localProjectPath = new File("").getAbsolutePath();
            {
                // 生成正集合,a:b:c:d=1:2:3:4
                FileWriter fw = new FileWriter(localProjectPath + "/input/email/num_pos.data");
                int[] x = { 10, 30, 60, 100 };
                for (int i = 0; i < 50000; i++) {
                    for (int j = 0; j < 100; j++) {
                        fw.write(splitRandom(rand.nextInt(100), a, x) + " ");
                    }
                    fw.write("\n");
                }
                fw.close();
            }
            {
                // 生成负集合,a:b:c:d=4:3:2:1
                FileWriter fw = new FileWriter(localProjectPath + "/input/email/num_neg.data");
                int[] x = { 40, 70, 90, 100 };
                for (int i = 0; i < 50000; i++) {
                    for (int j = 0; j < 100; j++) {
                        fw.write(splitRandom(rand.nextInt(100), a, x) + " ");
                    }
                    fw.write("\n");
                }
                fw.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String splitRandom(int x,
                                     String[] a,
                                     int[] b) {
        int i = 0;
        for (; i < b.length; i++) {
            if (x < b[i]) {
                break;
            } else {
                continue;
            }
        }
        return a[i];
    }

    public static void generateNumFile() {
        try {
            String localProjectPath = new File("").getAbsolutePath();
            Random rand = new Random();
            FileWriter fw = new FileWriter(localProjectPath + "/input/rand_numbers.data");
            for (int i = 0; i < 100; i++) {
                for (int j = 0; j < 10; j++) {
                    fw.write(rand.nextInt(1000) + "\t");
                }
                fw.write("\n");
            }
            fw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        generateGroupedNums();
    }
}

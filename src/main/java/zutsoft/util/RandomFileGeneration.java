package zutsoft.util;

import java.io.File;
import java.io.FileWriter;
import java.util.Random;

/**
 * 随机生成输入文件
 */
public class RandomFileGeneration {

    public static void generateNumFile() {
        try {
            String localProjectPath = new File("").getAbsolutePath();
            Random rand = new Random();
            FileWriter fw = new FileWriter(localProjectPath + "/input/rand_numbers.data");
            for (int i = 0; i < 100; i++) {
                for (int j = 0; j < 10; j++) {
                    fw.write(rand.nextInt(1000)+"\t");
                }
                fw.write("\n");
            }
            fw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        generateNumFile();
    }
}

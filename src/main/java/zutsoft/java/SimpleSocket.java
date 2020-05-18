package zutsoft.java;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

public class SimpleSocket {
    private static class Server extends Thread {
        @Override
        public void run() {
            ServerSocket ss = null;
            try {
                ss = new ServerSocket(7777);
                Socket s = ss.accept();
                BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
                PrintStream ps = new PrintStream(s.getOutputStream());
                while (true) {
                    try {
                        String line = br.readLine().trim();
                        if ("goodbye".equals(line)) {
                            break;
                        }
                        if (line.length() > 0) {
                            switch (line) {
                            case "你叫什么名字？":
                                ps.println("我叫笨笨");
                            case "你今年多大了?":
                                ps.println("我今年3岁");
                            default:
                                ps.println("对不起，我没有听懂你的意思，请重新提问");
                                break;
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                ss.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static class Client extends Thread {
        @Override
        public void run() {
            try {
                Socket s = new Socket("localhost", 7777);
                PrintStream ps = new PrintStream(s.getOutputStream());
                BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
                Scanner scanner = new Scanner(System.in);
                System.out.println(">>");
                while (true) {
                    String str = scanner.nextLine().trim();
                    ps.println(str);
                    if ("goodbye".equals(str)) {
                        break;
                    }
                    System.out.print("client收到：" + br.readLine() + "\n>>");
                }
                scanner.close();
                s.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        new Server().start();
        Thread.sleep(1000);
        new Client().start();
    }
}

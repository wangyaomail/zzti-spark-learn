package zutsoft.java;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;

public class StreamSocket {

    private static class Server extends Thread {
        public void run() {
            String[] errors = { "warn", "error", "info", "debug" };
            ServerSocket ss = null;
            try {
                Random rand = new Random();
                ss = new ServerSocket(7777);
                while (true) {
                    System.out.println("server ready.");
                    Socket s = ss.accept();
                    s.setSoTimeout(1);
                    PrintStream ps = new PrintStream(s.getOutputStream());
                    for (int i = 0; i < 100; i++) {
                        if (s.isConnected() && !s.isClosed()) {
                            try {
                                String msg = errors[rand.nextInt(4)] + ":" + i;
                                System.out.println(msg);
                                ps.println(msg);
                                Thread.sleep(700);
                            } catch (Exception e) {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                    ps.println("goodbye");
                    System.out.println("goodbye");
                    s.close();
                }
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
                BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
                while (true) {
                    try {
                        String line = br.readLine().trim();
                        System.out.println(line);
                        if ("goodbye".equals(line)) {
                            break;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        break;
                    }
                }
                s.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // new Server().start();
        // Thread.sleep(1000);
        new Client().start();
    }
}

package zutsoft.java;

import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;

public class FullStreamSocket {

    private static class Server extends Thread {
        public void run() {
            String[] errors = { "warn", "error", "info", "debug" };
            String[] urls = { "192.168.0.1", "192.168.0.2", "192.168.0.3", "192.168.0.4" };
            String[] requests = { "GET", "PUT", "POST", "DELETE" };
            String[] pages = { "/a", "/b", "/c", "/d" };
            String[] devices = { "pc", "web", "android", "ios" };
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
                                StringBuilder sb = new StringBuilder();
                                sb.append(i).append(" ");
                                sb.append(errors[rand.nextInt(4)]).append(" ");
                                sb.append(urls[rand.nextInt(4)]).append(" ");
                                sb.append(requests[rand.nextInt(4)]).append(" ");
                                sb.append(pages[rand.nextInt(4)]).append(" ");
                                sb.append(devices[rand.nextInt(4)]);
                                System.out.println(sb.toString());
                                ps.println(sb.toString());
                                Thread.sleep(100);
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

    public static void main(String[] args) throws Exception {
        new Server().start();
    }
}

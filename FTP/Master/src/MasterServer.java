import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class MasterServer {

    private static int CLIENT_PORT;
    private static int BROADCAST_PORT;
    private static int RESPONSE_PORT;
    private List<SlaveInfo> activeSlaves = new CopyOnWriteArrayList<>(); // Liste des slaves actifs
    private Map<String, List<List<SlaveInfo>>> fileLocations = new ConcurrentHashMap<>(); // Nom du fichier -> Liste de répliques par partie
    private final int REPLICATION_FACTOR = 2; // Nombre de copies par partie

    public Map<String, List<List<SlaveInfo>>> getFileLocations() {
        return fileLocations;
    }

    public List<SlaveInfo> getActiveSlaves() {
        return activeSlaves;
    }

    public static void main(String[] args) {
        String configFilePath = "E:\\FTP\\Master\\configMaster.properties";
        MasterServer masterServer = new MasterServer(configFilePath);
        try {
            masterServer.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public MasterServer(String configFilePath) {
        Properties properties = new Properties();
        try (InputStream input = new FileInputStream(configFilePath)) {
            properties.load(input);
            CLIENT_PORT = Integer.parseInt(properties.getProperty("clientPort"));
            BROADCAST_PORT = Integer.parseInt(properties.getProperty("broadcastPort"));
            RESPONSE_PORT = Integer.parseInt(properties.getProperty("responsePort"));
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public void start() throws IOException {
        System.out.println("MasterServer démarré sur le port " + CLIENT_PORT);

        try (ServerSocket serverSocket = new ServerSocket(CLIENT_PORT)) {
            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Client connecté : " + clientSocket.getInetAddress().getHostAddress());

                new Thread(new ClientHandler(this, clientSocket)).start();
            }
        }
    }

    public synchronized void discoverSlaves() {
        activeSlaves.clear();

        try (DatagramSocket socket = new DatagramSocket()) {
            socket.setBroadcast(true);

            String message = "DISCOVER_SLAVES";
            byte[] buffer = message.getBytes();

            DatagramPacket packet = new DatagramPacket(buffer, buffer.length, InetAddress.getByName("255.255.255.255"), BROADCAST_PORT);
            socket.send(packet);

            System.out.println("Message de broadcast envoyé pour découvrir les slaves.");

            // Attendre les réponses des slaves
            listenForSlaveResponses();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void listenForSlaveResponses() {
        try (DatagramSocket socket = new DatagramSocket(RESPONSE_PORT)) {
            socket.setSoTimeout(5000);
            byte[] buffer = new byte[1024];

            while (true) {
                try {
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    socket.receive(packet);
                    String message = new String(packet.getData(), 0, packet.getLength());
                    String slaveIp = packet.getAddress().getHostAddress();

                    if (message.startsWith("SLAVE_AVAILABLE")) {
                        String[] parts = message.split(":");
                        int slavePort = (parts.length > 1) ? Integer.parseInt(parts[1]) : 1235; // Parse custom port
                        activeSlaves.add(new SlaveInfo(slaveIp, slavePort));
                        System.out.println("Slave détecté: " + slaveIp + ":" + slavePort);
                    }
                } catch (SocketTimeoutException e) {
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int getFilePartitionCount(String filename) {
        return fileLocations.get(filename) == null ? 0 : fileLocations.get(filename).size();
    }

    public static int getClientPort() {
        return CLIENT_PORT;
    }

    public static void setClientPort(int clientPort) {
        CLIENT_PORT = clientPort;
    }

    public static int getBroadcastPort() {
        return BROADCAST_PORT;
    }

    public static void setBroadcastPort(int broadcastPort) {
        BROADCAST_PORT = broadcastPort;
    }

    public static int getResponsePort() {
        return RESPONSE_PORT;
    }

    public static void setResponsePort(int responsePort) {
        RESPONSE_PORT = responsePort;
    }

    public void setActiveSlaves(List<SlaveInfo> activeSlaves) {
        this.activeSlaves = activeSlaves;
    }

    public void setFileLocations(Map<String, List<List<SlaveInfo>>> fileLocations) {
        this.fileLocations = fileLocations;
    }

    public int getREPLICATION_FACTOR() {
        return REPLICATION_FACTOR;
    }
}




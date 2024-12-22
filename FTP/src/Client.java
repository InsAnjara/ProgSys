import java.io.*;
import java.net.Socket;
import java.util.Scanner;

public class Client {
    private String serverIp;
    private int serverPort;

    public Client(String serverIp, int serverPort) {
        this.serverIp = serverIp;
        this.serverPort = serverPort;
    }

    public void start() {
        try (Socket socket = new Socket(serverIp, serverPort);
             DataInputStream dis = new DataInputStream(socket.getInputStream());
             DataOutputStream dos = new DataOutputStream(socket.getOutputStream())) {

            System.out.println("Connecté au serveur : " + serverIp + ":" + serverPort);

            Scanner scanner = new Scanner(System.in);
            boolean running = true;

            while (running) {
                try {
                    System.out.println("\n=== Menu Client ===");
                    System.out.println("1. Lister les fichiers");
                    System.out.println("2. Ajouter un fichier");
                    System.out.println("3. Télécharger un fichier");
                    System.out.println("4. Supprimer un fichier");
                    System.out.println("5. Quitter");
                    System.out.print("Choisissez une option : ");

                    int choix = scanner.nextInt();
                    scanner.nextLine();

                    switch (choix) {
                        case 1:
                            listFiles(dos, dis);
                            break;
                        case 2:
                            addFile(dos, dis, scanner);
                            break;
                        case 3:
                            getFile(dos, dis, scanner);
                            break;
                        case 4:
                            removeFile(dos, dis, scanner);
                            break;
                        case 5:
                            dos.writeUTF("QUIT");
                            running = false;
                            break;
                        default:
                            System.out.println("Option invalide.");
                    }
                } catch (IOException e) {
                    System.err.println("Erreur de communication avec le serveur : " + e.getMessage());
                    running = false;
                }
            }
        } catch (IOException e) {
            System.err.println("Erreur de connexion : " + e.getMessage());
        }
    }

    // Modifier la méthode listFiles :
    private void listFiles(DataOutputStream dos, DataInputStream dis) throws IOException {
        dos.writeUTF("LIST");
        dos.flush();

        int fileCount = dis.readInt();
        if (fileCount == 0) {
            System.out.println("Aucun fichier sur le serveur.");
            return;
        }

        System.out.println("\nFichiers disponibles sur le serveur :");
        for (int i = 0; i < fileCount; i++) {
            String fileName = dis.readUTF();
            int partCount = dis.readInt();
            System.out.println("- " + fileName + " (" + partCount + " parties)");
        }
    }

    private void removeFile(DataOutputStream dos, DataInputStream dis, Scanner scanner) throws IOException {
        System.out.print("Entrez le nom du fichier à supprimer : ");
        String fileName = scanner.nextLine();

        dos.writeUTF("REMOVE");
        dos.writeUTF(fileName);

        String response = dis.readUTF();
        System.out.println(response);
    }

    private void addFile(DataOutputStream dos, DataInputStream dis, Scanner scanner) throws IOException {
        System.out.print("Entrez le chemin du fichier à envoyer : ");
        String filePath = scanner.nextLine();

        File file = new File(filePath);
        if (!file.exists() || !file.isFile()) {
            System.out.println("Fichier introuvable. Vérifiez le chemin.");
            return;
        }

        dos.writeUTF("ADD");
        FileTransferUtils.sendFile(dos, filePath);

        String serverResponse = dis.readUTF();
        System.out.println(serverResponse);
        if (serverResponse.startsWith("SUCCESS")) {
            System.out.println("Fichier ajouté avec succès.");
        } else {
            System.out.println("Échec de l'ajout. Fichier déjà présent sur le serveur.");
        }
    }

    private void getFile(DataOutputStream dos, DataInputStream dis, Scanner scanner) throws IOException {
        System.out.print("Entrez le nom du fichier à télécharger : ");
        String fileName = scanner.nextLine();

        dos.writeUTF("GET");
        dos.writeUTF(fileName);

        String response = dis.readUTF();
        if (response.startsWith("Erreur")) {
            System.out.println("Fichier non trouvé sur le serveur.");
            return;
        }

        File newFile = FileTransferUtils.receiveFile(dis, "./downloads");
        System.out.println("Fichier : " + newFile.getAbsolutePath() + " téléchargé avec succès .");
    }

    public static void main(String[] args) {
        String serverIp = "127.0.0.1";
        int serverPort = 1234;
        new Client(serverIp, serverPort).start();
    }
}

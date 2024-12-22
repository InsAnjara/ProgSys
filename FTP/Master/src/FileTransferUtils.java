import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class FileTransferUtils {
    public static File receiveFile(DataInputStream dis, String saveDirectory) throws IOException {
        // 1. Lire le nom du fichier
        String fileName = dis.readUTF();

        // 2. Lire la taille du fichier
        long fileSize = dis.readLong();

        // 3. Lire le checksum
        String expectedChecksum = dis.readUTF();

        // Créer le répertoire de sauvegarde s'il n'existe pas
        File directory = new File(saveDirectory);
        if (!directory.exists() && !directory.mkdirs()) {
            throw new IOException("Impossible de créer le répertoire : " + saveDirectory);
        }

        // Chemin complet pour sauvegarder le fichier
        File saveFile = new File(directory, fileName);

        // Buffer pour stocker temporairement les données
        byte[] buffer = new byte[4096];
        int bytesRead;
        long totalRead = 0;

        try (FileOutputStream fos = new FileOutputStream(saveFile);
             BufferedOutputStream bos = new BufferedOutputStream(fos)) {

            System.out.println("Début de la réception des données...");

            while (totalRead < fileSize) {
                bytesRead = dis.read(buffer, 0, (int) Math.min(buffer.length, fileSize - totalRead));
                if (bytesRead == -1) {
                    throw new IOException("Fin du flux inattendue");
                }
                bos.write(buffer, 0, bytesRead);
                totalRead += bytesRead;

                // Log de progression optionnel
                if (totalRead % (1024 * 1024) == 0) { // Log tous les 1MB
                    System.out.println("Progression : " + (totalRead * 100 / fileSize) + "%");
                }
            }
            bos.flush();
        }

        System.out.println("Fichier reçu, vérification du checksum...");

        // Vérifier le checksum du fichier reçu
        String receivedChecksum = calculateChecksum(saveFile.getAbsolutePath());
        System.out.println("Checksum calculé : " + receivedChecksum);

        if (!expectedChecksum.equals(receivedChecksum)) {
            if (!saveFile.delete()) {
                System.err.println("Impossible de supprimer le fichier corrompu : " + saveFile.getAbsolutePath());
            }
            throw new IOException("Checksum invalide. Attendu: " + expectedChecksum + ", Reçu: " + receivedChecksum);
        }

        System.out.println("Fichier reçu et validé avec succès");
        return saveFile;
    }

    public static void sendFile(DataOutputStream dos, String filePath) throws IOException {
        File file = new File(filePath);
        if (!file.exists() || !file.isFile()) {
            throw new FileNotFoundException("Fichier introuvable : " + filePath);
        }

        System.out.println("Envoi du fichier : " + file.getName());

        try {
            // Calculer le checksum avant l'envoi
            String checksum = calculateChecksum(filePath);
            System.out.println("Checksum calculé : " + checksum);

            // Envoyer le nom du fichier
            dos.writeUTF(file.getName());
            dos.flush();

            // Envoyer la taille du fichier
            dos.writeLong(file.length());
            dos.flush();

            // Envoyer le checksum
            dos.writeUTF(checksum);
            dos.flush();

            // Envoyer le contenu du fichier
            try (FileInputStream fis = new FileInputStream(file);
                 BufferedInputStream bis = new BufferedInputStream(fis)) {

                byte[] buffer = new byte[4096];
                int bytesRead;
                long totalSent = 0;

                while ((bytesRead = bis.read(buffer)) > 0) {
                    dos.write(buffer, 0, bytesRead);
                    totalSent += bytesRead;

                    // Log de progression optionnel
                    if (totalSent % (1024 * 1024) == 0) { // Log tous les 1MB
                        System.out.println("Progression : " + (totalSent * 100 / file.length()) + "%");
                    }
                }
                dos.flush();
            }

            System.out.println("Fichier envoyé avec succès");
        } catch (IOException e) {
            System.err.println("Erreur lors de l'envoi du fichier : " + e.getMessage());
            throw e;
        }
    }

    public static String calculateChecksum(String filePath) throws IOException {
        try (FileInputStream fis = new FileInputStream(filePath)) {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            byte[] buffer = new byte[4096];
            int bytesRead;

            while ((bytesRead = fis.read(buffer)) > 0) {
                digest.update(buffer, 0, bytesRead);
            }

            byte[] checksumBytes = digest.digest();
            StringBuilder checksum = new StringBuilder();
            for (byte b : checksumBytes) {
                checksum.append(String.format("%02x", b));
            }
            return checksum.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Algorithme de hashage MD5 introuvable.", e);
        }
    }

    public static File[] splitFile(String filePath, int numParts) throws IOException {
        File file = new File(filePath);
        if (!file.exists() || !file.isFile()) {
            throw new FileNotFoundException("Fichier introuvable : " + filePath);
        }

        long fileSize = file.length();
        long partSize = fileSize / numParts + ((fileSize % numParts == 0) ? 0 : 1);

        File[] parts = new File[numParts];
        try (FileInputStream fis = new FileInputStream(file)) {
            byte[] buffer = new byte[4096];

            for (int i = 0; i < numParts; i++) {
                parts[i] = new File(file.getParent(), file.getName() + ".part" + (i + 1));
                try (FileOutputStream fos = new FileOutputStream(parts[i])) {
                    long bytesWritten = 0;
                    int bytesRead;
                    while (bytesWritten < partSize && (bytesRead = fis.read(buffer)) > 0) {
                        fos.write(buffer, 0, bytesRead);
                        bytesWritten += bytesRead;
                    }
                }
            }
        }
        return parts;
    }

    public static void deleteDirectory(String directoryPath) {
        File directory = new File(directoryPath);

        if (!directory.exists()) {
            System.out.println("Directory does not exist: " + directoryPath);
            return;
        }

        if (!directory.isDirectory()) {
            if (directory.delete()) {
                System.out.println("Deleted file: " + directoryPath);
            } else {
                System.err.println("Failed to delete file: " + directoryPath);
            }
            return;
        }

        System.out.println("Deleting directory: " + directoryPath);

        // Recursively delete files and subdirectories
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file.getAbsolutePath()); // Recursive call for subdirectory
                } else {
                    if (file.delete()) {
                        System.out.println("Deleted file: " + file.getAbsolutePath());
                    } else {
                        System.err.println("Failed to delete file: " + file.getAbsolutePath());
                    }
                }
            }
        }

        // Delete the directory itself
        if (directory.delete()) {
            System.out.println("Deleted directory: " + directory.getAbsolutePath());
        } else {
            System.err.println("Failed to delete directory: " + directory.getAbsolutePath());
        }
    }
}

package org.example.Console;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

public class RestConsoleApp {

    public static void main(String[] args) {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            System.out.println("Entrez la commande (par exemple, Get_global_values):");
            String command = reader.readLine();

            if ("Get_global_values".equalsIgnoreCase(command)) {
                String result = sendRestRequest("http://localhost:8080/command", command);
                System.out.println("Résultat: " + result);
            } else {
                System.out.println("Commande non reconnue");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String sendRestRequest(String urlStr, String command) throws IOException {
        URL url = new URL(urlStr);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();

        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setDoOutput(true);

        String jsonBody = String.format("{\"command\": \"%s\"}", command);

        try (OutputStream os = connection.getOutputStream()) {
            os.write(jsonBody.getBytes());
            os.flush();
        }

        int responseCode = connection.getResponseCode();
        if (responseCode == HttpURLConnection.HTTP_OK) {
            BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String inputLine;
            StringBuilder content = new StringBuilder();

            while ((inputLine = in.readLine()) != null) {
                content.append(inputLine);
            }

            in.close();
            connection.disconnect();
            return content.toString();
        } else {
            throw new IOException("Erreur lors de l'envoi de la requête REST: " + responseCode);
        }
    }
}

package org.example.Console;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

public class RestConsoleApp {

    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String command = "";
        String output = "";

        while (!"exit".equalsIgnoreCase(command)) {
            System.out.println("Voici les commandes disponibles :\n" +
                    "Get_global_values : retourne les valeurs globales clés Global du fichier json\n" +
                    "Get_country_values <Pays> : retourne les valeurs du pays demandé\n" +
                    "Get_confirmed_avg : retourne une moyenne des cas confirmés\n" +
                    "Get_deaths_avg : retourne une moyenne des Décès\n" +
                    "Get_countries_deaths_percent : retourne le pourcentage de Décès par rapport aux cas confirmés");
            command = br.readLine();

            switch (command) {
                case "Get_global_values":
                    output = sendRestRequest("http://localhost:8080/command", command);
                    break;
                case "Get_country_values Algerie":
                    output = "?";
                    break;
                case "Get_confirmed_avg":
                    output = "?";
                    break;
                case "Get_deaths_avg":
                    output = "?";
                    break;
                case "Get_countries_deaths_percent":
                    output = "?";
                    break;
                default:
                    System.out.println("Commande non reconnue");
                    output = "";
            }

            if (!output.isEmpty()) {
                System.out.println(output);
            }
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

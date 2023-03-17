package org.example.Console;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class RestConsoleApp {

    public static void main(String[] args) throws IOException, InterruptedException {
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
                    output = sendRestRequest("http://localhost:8080/command", command);
                    break;
                case "Get_confirmed_avg":
                    output = sendRestRequest("http://localhost:8080/command", command);
                    break;
                case "Get_deaths_avg":
                    output = sendRestRequest("http://localhost:8080/command", command);
                    break;
                case "Get_countries_deaths_percent":
                    output = sendRestRequest("http://localhost:8080/command", command);
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

    private static String sendRestRequest(String urlStr, String command) throws IOException, InterruptedException {
        var value = command;
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(urlStr))
                .POST(HttpRequest.BodyPublishers.ofString(value))
                .build();

        HttpResponse<String> response = client.send(request,
                HttpResponse.BodyHandlers.ofString());
        return response.body();
    }

}

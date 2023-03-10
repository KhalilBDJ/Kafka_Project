# Kafka_Project

Pour charger le serveur Kafka, il faut utiliser docker. Pour charger le fichier YAML, il faut se rendre dans KAFKA_Env et entrer la commande : docker-compose up -d

Une fois exécuté, vous pour lancer les commandes :

docker exec -it zookeeper /bin/bash (Pour lancer la console du serveur Zookeeper) 

docker exec -it kafka-1 /bin/bash (Pour lancer la console du broker kafka-1)

docker exec -it kafka-2 /bin/bash (Pour lancer la console du broker kafka-2)

Pour faire un consumer:

kafka-console-consumer --bootstrap-server localhost:9092 --topic my-topic --from-beginning

Pour supprimer les messages :

kafka-delete-records --bootstrap-server localhost:9092 --topic my-topic --offset earliest

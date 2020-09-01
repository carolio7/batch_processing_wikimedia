# batch_processing_wikimedia

![schéma architecture fonctionnelle](https://github.com/carolio7/batch_processing_wikimedia/blob/master/functional_architecture.png)

### 1. Présentation du sujet:
Pour identifier les personnes ayant le plus contributées à un thème pointu donné, on va créer notre propre data lake à partir des historiques de pages de Wikipedia.
Et comme, il peut y avoir plusieurs articles liées sur un thème donné, on utilisera aussi les liens entre les pages pour construire une structure orienté graphe. Dans cette graphe, les pages représentent les noeuds et les liens sont constitués des pages qui pointent vers notre page ou les liens que notre page utilise.


### 2. Etapes :
        - télecharger nos données brutes à partir de wikipedia
        - Creer la struture de notre data lake
        - charger les données brutes dans notre data lake HDFS
        - sérialiser nos données brutes par lots avec avro et python
        - Sécuriser notre data lake pour ne pas perdre nos données un jour
        - utiliser spark SQL pour interroger nos données sérialisées en fonction de nos besoins


###### 2. a) Télechargez nos input (données brutes) :
        L'historique des pages et les pages links sont à prendre sur [Wikipedia](https://dumps.wikimedia.org/frwiki/):
            - frwiki-20200201-stub-meta-history.xml : contient les info sur toutes les pages (75,9 Go)
            - frwiki-20200201-pagelinks.sql : contient les liens entre les pages (11,9 Go)


###### 2. b) Créez la structure de notre data lake :
         Utiliser les commande "$ ./bin/hdfs dfs -ls /répertoire" pour contruire la struture de HDFS comme dans [P4_01_document.pdf](./livrables/P4_01_document.pdf)


###### 2. c) Chargez les données brutes dans notre data lake HDFS :
        Commande à utiliser : $ ./bin/hdfs dfs -copyFromLocal /repertoire_input/* /data/frwiki/raw/


###### 2. d) Sérialisez nos données brutes par lots avec avro et python :
        Sérialisation de pages history: time python ./history_serialization_014.py /data/frwiki/raw /data/frwiki/frwiki-20200201/master/full
        Sérialisation de pages links:   time python ./pagelinks_hdfs_serialization_13.py /data/frwiki/raw /data/frwiki/frwiki-20200201/master/full


###### 2. e) Sécurisez nos données dans notre data lake:
                I- Utilisation des snapshot: les snapshots permettent de remettre des fichiers ou répertoires à un état sauvegardé précédemment.
                    - Rendre le repertoire "/data/" et tout ce qu'il y à l'intérieur snapshotable: $ ./bin/hdfs dfsadmin -allowSnapshot /data
                    - Créer un snapshot à un moment donnée: $ ./bin/hdfs dfs -createSnapshot /data
                    - Si un jours, on veut restaurer des repertoire ou fichier à un état précedent, il faut la copier à partir de : $ ./bin/hdfs dfs -cp -f /data/.snapshot/snapshot1/* /data/

                II- Verouiller l'acces en écriture de nos données brutes et celles sérialisées : hdfs dfs -chmod -R ugo-w /data/frwiki/raw /data/frwiki/frwiki-20200201/master/full


###### 2. f) Utilisation de spark SQL pour faire des requêtes sur notre jeux de données:
                Utiliser le script spark SQL "queryScript_003.py" pour chercher les plus grands contributeurs à un sujet donnée.
                Le script s'utilise comme ceci-ci: time spark-submit ./queryScript_003.py titre_theme_à_chercher



### 3. Résultats:
        Si on recherche les trois meilleurs contributeur du cinéma surréaliste, on obtiendrait le résultat ci-dessous:

![résultat](https://github.com/carolio7/batch_processing_wikimedia/blob/master/resultats.png)

+-----------+-------+----+-----+
|       name|     id|  ip|count|
+-----------+-------+----+-----+
|     Léon66| 100556|null|  234|
|      Jolek| 862133|null|  205|
|Pierrette13|1220016|null|  133|
+-----------+-------+----+-----+


### 4. Performance:
    Avec ma machine:
                - Lenovo ThinkPad-Edge-E330
                - SE: Ubuntu 16.04 LTS
                - Mémoire: 7,4 Gio
                - Procésseur: Intel® Core™ i5-3210M CPU @ 2.50GHz × 4

    Il a fallut environ :
        - 8 heures pour la sérialisation de l'history pages (75,9 Go)
        - 37 heures pour la sérialisation de la pagelinks (11,9 Go)
        - 8 heures pour interroger les meilleurs contributeurs avec Spark SQL

### 5. Conclusion:
        - Temps de sérialisation incohérent entre pages link et history vient du fait qu'on a trop entassé les données dans pagelinks.
            On arrive avec moins fichiers avro mais on a perdu la performance en temps.
        - Interessant d'augmenter le nombre d'executor spark pour optimiser le traitement des requêtes.

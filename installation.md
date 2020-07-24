Avant de pouvoir lancer nos applications, il faut d'abord installer les outils nécessaires à son fonctionnement.
La méthode d'installation décrite dans ce fichier concerne le Ubuntu 16.04

## I- Installation de HDFS hadoop:
1- HDFS a été programmé en java, il faut avoir un JRE sur la machine pour l'utiliser.
    "sudo apt-get install default-jre"
    A verifier l'installation avec la commande: "which java"
    
2- Définir la variable d'environnement dans le fichier "~/.bashrc" en ajoutant la ligne:
        export JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:bin/java::")
        
3- Télechargez hadoop depuis le site officiel:
        "wget https://downloads.apache.org/hadoop/common/hadoop-2.10.0/hadoop-2.10.0.tar.gz"
    Décompressez l'archive obtenu et se placer à l'intérieur.
        "$ cd ./hadoop-2.10.0"
    
4- Configuration du cluster:
    4.a) Définir le port d'écoute à partir dans le fichier "core-site.xml"
        $ vim etc/hadoop/core-site.xml
            <configuration>
                <property>
                    <name>fs.defaultFS</name>
                    <value>hdfs://localhost:8000</value>
                </property>
            </configuration>

    4.b) Définir l'emplacement de stockage du namenode et datanode à partir du fihier "hdfs-site.xml".
        Ceci se fait après avoir créer les deux répertoire "~/hdfs/namenode/" et "~/hdfs/datanode/".
        $ vim etc/hadoop/hdfs-site.xml
            <configuration>
                <property>
                    <name>dfs.name.dir</name>
                    <value>~/hdfs/namenode/</value>
                </property>
                <property>
                    <name>dfs.data.dir</name>
                    <value>~/hdfs/datanode/</value>
                </property>
            </configuration>

Remarque: on peut simuler deux ou trois noeuds datanode mais comme cette simulation sera en local, la replication des données de notre data lake sur une même machine risque d'encombrer l'espace dispo du disque.

    4.c) Formattez le namenode avec la commande: $ ./bin/hdfs namenode -format

    4.d) Demarrez le cluster avec les commandes suivantes:
            $ ./bin/hdfs namenode
            $ ./bin/hdfs datanode



## II- Installation de Spark:
    En supposant que python et Pypi sont déjà intallé sur votre machine.
    Installation de pyspark par pip se fait avec:
        "pip install pyspark"

    Ou si vous comptez utiliser spark avec un autre language que python, il faut télécharger le package:
        $ wget https://downloads.apache.org/spark/spark-2.4.6/spark-2.4.6-bin-hadoop2.7.tgz
        Décompressez le et se placer dans le "./bin" à l'intérieur du pack.



## III- Installation de fastavro pour la sérialisation des données avec Python:
    L'installation de fastavro avec Pypi est très simple:  pip install fastavro
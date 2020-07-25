#! /usr/bin/env python3
# -*- coding:Utf8 -*-

# Launch this script with command: 
#   time python ./pagelinks_hdfs_serialization_13.py /data/frwiki/raw /data/frwiki/frwiki-20200201/master/full


# serialization de pagelink

import os, sys, json, re

import fastavro
import hdfs


def main():
    # Lecture des arguments venant de ligne de commande
    src_dir = sys.argv[1]
    dest_dir = sys.argv[2]


    # Lecture du schéma
    schema = json.load(open(os.path.join(os.path.dirname(__file__),"pagelink.avsc")))

    # Création du client HDFS
    hdfs_client = hdfs.InsecureClient("http://0.0.0.0:50070")


    # List files
    for filename in hdfs_client.list(src_dir):
        if os.path.splitext(filename)[1] != ".sql":
            continue

        sql_path = os.path.join(src_dir, filename)
        avro_path = os.path.join(dest_dir, filename[:-4])


        print("Traitement ", avro_path)
        serialize(sql_path, avro_path, hdfs_client, schema)
        



def serialize(sql_path, avro_path, hdfs_client, schema):
    with hdfs_client.read(sql_path) as sql_file:

        insert_regex = re.compile('''INSERT INTO `pagelinks` VALUES (.*)\;''')
        row_regex = re.compile("""(.*),(.*),'(.*)',(.*)""")
        avro_content = []
        MAX_AVROCONTENT_LENGTH = 1000000 # Constante à mettre à jour selon la capacité de la machine
        numero_output = 1

        for line_bytes in sql_file:
            #line = str(line_bytes, 'utf-8')
            line = str(line_bytes, encoding="ISO-8859-1")
            match = insert_regex.match(line.strip())
            if match is not None:
                
                data = match.groups(0)[0]
                rows = data[1:-1].split("),(")
                for row in rows:
                    row_match = row_regex.match(row)
                    if row_match is not None:
                        pl_from = row_match.groups()[0]
                        pl_namespace = row_match.groups()[1]
                        pl_title = row_match.groups()[2]
                        pl_from_namespace = row_match.groups()[3]

                        avro_content.append({"pl_from":int(pl_from), "pl_namespace":int(pl_namespace), "pl_title": pl_title, "pl_from_namespace": int(pl_from_namespace)})
                        
                        if len(avro_content) > MAX_AVROCONTENT_LENGTH :
                            avro_file = avro_path +'-'+ str(numero_output).zfill(3) + '.avro'
                            sauvegarder_liste(hdfs_client, avro_file, schema, avro_content)
                            del avro_content    # Liberer la memoire
                            avro_content = []
                            numero_output += 1


        print('Ecriture du dernier liste dans HDFS')
        avro_file = avro_path +'-'+ str(numero_output).zfill(3) + '.avro'
        sauvegarder_liste(hdfs_client, avro_file, schema, avro_content)
    print('Fin de la sérialisation, au revoir !!!')



def sauvegarder_liste(hdfs_client, avro_file, schema, avro_content):
    print('Ecriture dans HDFS de : ', avro_file)
    with hdfs_client.write(avro_file,overwrite=True) as f:
        fastavro.writer(f, schema, avro_content)




if __name__ == "__main__":
    main()
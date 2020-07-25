#! /usr/bin/env python3
# -*- coding:Utf8 -*-

# Launch this script with command: 
#   time spark-submit ./queryScript_003.py CiNéma surRéaliste
# Vous pouvez modifier l'argument "cinema surrealiste" à votre guise

import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql import SQLContext


sc = SparkContext()
spark = SparkSession.builder.config("spark.sql.broadcastTimeout", "36000").getOrCreate()


# Exemple du mot à chercher
mot_cle = 'cinéma surréaliste'.lower()
mot_cle = str(sys.argv[1]).lower()
print("*********************************************************")
print("On va chercher les meilleurs contributeur à : ", mot_cle)
print("*********************************************************")


# Chager les fichiers avro history contribition
path_contribHistory = "hdfs://localhost:8000/data/frwiki/frwiki-20200201/master/full/frwiki-20200201-stub-meta-history-*"
df_contribHistory = spark.read.format("avro").load(path_contribHistory)
df_contribHistory.printSchema()
df_contribHistory.createOrReplaceTempView("pagesHistory")


# Charger les fichier pageslinks
path_pagelink = 'hdfs://localhost:8000/data/frwiki/frwiki-20200201/master/full/frwiki-20200201-pagelinks-*'
df_pl = spark.read.format("avro").load(path_pagelink)
df_pl.printSchema()

df_pl2 = df_pl.drop("pl_namespace").drop("pl_from_namespace").withColumnRenamed("pl_from", "src").withColumnRenamed("pl_title", "dest_title")
df_pl2.printSchema()
df_pl2.createOrReplaceTempView("pageslink")


# Recherche du mot-clé dans le titre de history
print("*********************************************************")
print("Construction du 1er dataframe: # Recherche du mot-clé dans le titre de history")
premier_df = spark.sql("""
    SELECT p_id, p_title, p_revisions FROM pagesHistory WHERE LOWER(p_title) LIKE '%{}%'
    """.format(mot_cle))
premier_df.persist()
premier_df.createOrReplaceTempView("premier_result")
premier_df.show()


# Les liens sortants de notre page
print("*********************************************************")
print("Construction de la 2eme dataframe: # Les liens sortants de notre page")
deuxieme_df = spark.sql("""
    SELECT p_id, p_title, p_revisions FROM pagesHistory ph INNER JOIN
    (
        SELECT pageslink.dest_title FROM premier_result INNER JOIN pageslink 
        ON (premier_result.p_id = pageslink.src)
    ) pUsingOurLink ON (ph.p_title = pUsingOurLink.dest_title)
    """)
deuxieme_df.persist()
deuxieme_df.createOrReplaceTempView("deuxieme_result")
deuxieme_df.show()



# Les liens entrants dans notre page
print("*********************************************************")
print("Construction de la 3eme dataframe: # Les liens entrants dans notre page actuelle")
troisieme_df = spark.sql("""
    SELECT p_id, p_title, p_revisions FROM pagesHistory ph 
    INNER JOIN 
    (
        SELECT pageslink.src FROM pageslink
        WHERE LOWER(dest_title) LIKE '%{}%'
    ) linkThatOurPageUse 
    ON (ph.p_id = linkThatOurPageUse.src)
    """.format(mot_cle))
troisieme_df.persist()
troisieme_df.createOrReplaceTempView("troisieme_result")
troisieme_df.show()


# Vertical union des trois dataframes
print("*********************************************************")
print("Union des trois dataframes")
#union_df = premier_df.union(deuxieme_df).union(troisieme_df)
union_df = spark.sql("""
        (SELECT p_revisions.r_contributor FROM premier_result)
        UNION ALL
        (SELECT p_revisions.r_contributor FROM deuxieme_result)
        UNION ALL
        (SELECT p_revisions.r_contributor FROM troisieme_result)
    """)
union_df.persist()
union_df.show()



print("*********************************************************")
print("Extraction des contributeurs : ")
from pyspark.sql.functions import explode
df_exploded = union_df.select(explode(union_df.r_contributor))
contributor_df = df_exploded.select(df_exploded.col.r_username, df_exploded.col.r_contributor_id ,df_exploded.col.r_contributor_ip)
contributor_df = contributor_df.withColumnRenamed('col.r_username', 'name')\
                                .withColumnRenamed('col.r_contributor_id', 'id')\
                                .withColumnRenamed('col.r_contributor_ip', 'ip')


print("*********************************************************")
print("Voici les meilleurs contributeurs : ")
contributor_count_df = contributor_df.groupby(["name", "id", "ip"]).count().orderBy("count", ascending=False)
contributor_count_df.show()



print("************************ FIN *********************************")
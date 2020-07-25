#! /usr/bin/env python3
# -*- coding:Utf8 -*-


# Launch this script with command: 
#   time python ./history_serialization_014.py /data/frwiki/raw /data/frwiki/frwiki-20200201/master/full

# serialization de history

import os, sys, json, re
import xml.etree.ElementTree as ET

import fastavro
import hdfs


def main():
    # Lecture des arguments venant de ligne de commande
    src_dir = sys.argv[1]
    dest_dir = sys.argv[2]


    # Lecture du schéma
    schema = json.load(open(os.path.join(os.path.dirname(__file__),"pageshistory.avsc")))

    # Création du client HDFS
    hdfs_client = hdfs.InsecureClient("http://0.0.0.0:50070")


    # List files
    for filename in hdfs_client.list(src_dir):
        if os.path.splitext(filename)[1] != ".xml":
            continue

        xml_path = os.path.join(src_dir, filename)
        avro_path = os.path.join(dest_dir, filename[:-4])


        print("Traitement ", avro_path)
        serialize(xml_path, avro_path, hdfs_client, schema)
        


def serialize(xml_path, avro_path, hdfs_client, schema):
    with hdfs_client.read(xml_path) as xml_file:

        pages = []
        MAX_PAGES_LENGTH = 750 # Constante à mettre à jour selon la capacité de la machine
        numero_output = 1

        # On va appliquer la lecture avec namespace du XML
        xmlns = 'http://www.mediawiki.org/xml/export-0.10/'
        context = ET.iterparse(xml_file, events=("start", "end"))
        # turn it into an iterator
        context = iter(context)
        event, root = next(context)
        for event, elem in context:
            if event == 'end' and elem.tag == ET.QName(xmlns, 'page'):
                #p=extract_infosPages(elem, xmlns)
                pages.append(extract_infosPages(elem, xmlns))

                if len(pages) > MAX_PAGES_LENGTH:
                    root.clear()
                    avro_file = avro_path + '-'+ str(numero_output).zfill(5) + '.avro'
                    sauvegarder_liste(hdfs_client, avro_file,schema, pages)
                    numero_output += 1
                    del pages
                    pages = []


        print('Dernier serialization')
        # On sauvegarde ce qui reste dans page et qui n'a été écrite dans HDFS
        avro_file = avro_path + '-'+ str(numero_output).zfill(5) + '.avro'
        sauvegarder_liste(hdfs_client, avro_file,schema, pages)
        print('Fin de la sérialisation, au revoir !!!')





def extract_infosPages(element, xmlns):
    page = {}
    p_title = None
    p_namespace = None
    p_id = None
    p_revisions = []
    for page_child in element:
        if page_child.tag == ET.QName(xmlns, 'title'):
            p_title = page_child.text
        elif page_child.tag == ET.QName(xmlns, 'ns'):
            p_namespace = int(page_child.text)
        elif page_child.tag == ET.QName(xmlns, 'id'):
            p_id = int(page_child.text)
        elif page_child.tag == ET.QName(xmlns, 'revision'):
            revision = extract_revisons(page_child, xmlns)
            p_revisions.append(revision)

    page = {
        'p_title': p_title,
        'p_namespace': p_namespace,
        'p_id': p_id,
        'p_revisions': p_revisions
    }
    return page





def extract_revisons(balise_revision, xmlns):
    revision = {}
    r_id = r_parent_id = r_timestamp = r_minor = None
    r_comment = r_model = r_format = r_text = r_sha1 = None
    for rev_child in balise_revision:
        if rev_child.tag == ET.QName(xmlns, 'id'):
            r_id = int(rev_child.text)
        elif rev_child.tag == ET.QName(xmlns, 'parentid'):
            r_parent_id = int(rev_child.text) if rev_child.text is not None else -911
        elif rev_child.tag == ET.QName(xmlns, 'timestamp'):
            r_timestamp = rev_child.text
        elif rev_child.tag == ET.QName(xmlns, 'contributor'):
            r_contributor = extract_contributor(rev_child, xmlns)
            
        elif rev_child.tag == ET.QName(xmlns, 'minor'):
            r_minor = rev_child.text
        elif rev_child.tag == ET.QName(xmlns, 'comment'):
            r_comment = rev_child.text
        elif rev_child.tag == ET.QName(xmlns, 'model'):
            r_model = rev_child.text
        elif rev_child.tag == ET.QName(xmlns, 'format'):
            r_format = rev_child.text
        elif rev_child.tag == ET.QName(xmlns, 'text'):
            r_text = {'r_text_bytes': int(rev_child.get('bytes')) if None is not rev_child.get('bytes') else -911,
                        'r_text_id': int(rev_child.get('id')) if None is not rev_child.get('id') else -911}
        elif rev_child.tag == ET.QName(xmlns, 'sha1'):
            r_sha1 = rev_child.text
            
    revision = {
        'r_id': r_id,
        'r_parent_id': r_parent_id,
        'r_timestamp': r_timestamp,
        'r_contributor': r_contributor,
        'r_minor': r_minor,
        'r_comment': r_comment,
        'r_model': r_model,
        'r_format': r_format,
        'r_text': r_text,
        'r_sha1': r_sha1
    }

    return revision






def extract_contributor(balise_contributor, xmlns):
    contributeur = {}
    r_username = None
    r_user_id = None
    r_user_ip = None
    for contrib_child in balise_contributor:
        if contrib_child.tag == ET.QName(xmlns, 'username'):
            r_username = contrib_child.text
        elif contrib_child.tag == ET.QName(xmlns, 'id'):
            r_user_id = int(contrib_child.text)
        elif contrib_child.tag == ET.QName(xmlns, 'ip'):
            r_user_ip = contrib_child.text
                    
    contributeur = {
        "r_username" : r_username,
        "r_contributor_id": r_user_id,
        'r_contributor_ip': r_user_ip
    }

    return contributeur





def sauvegarder_liste(hdfs_client, avro_file, schema, avro_content):
    print('Ecriture dans HDFS de : ', avro_file)
    with hdfs_client.write(avro_file,overwrite=True) as f:
        fastavro.writer(f, schema, avro_content)



if __name__ == "__main__":
    main()
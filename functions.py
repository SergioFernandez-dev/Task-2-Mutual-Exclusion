#!/bin/env python

import pywren_ibm_cloud as pywren
import time as t
import json
import pickle

N_SLAVES = 5
BUCKET_NAME = 'depositosergiofernandez'
name = 'write_'
name2 = 'p_write_'
name3 = 'result.json'
name4 = 'pywren.jobs'
time = 0.1
data = []
get_last_modified = lambda obj: int(obj['LastModified'].timestamp())


def master(x, ibm_cos):
    cont = 0
    write_permission_list = []
    while cont < N_SLAVES:

        # Agafem una llista de tots els fitxers p_write_id i els ordenem pel temps que fa que estan creats
        lista = ibm_cos.list_objects(Bucket=BUCKET_NAME, Prefix=name2)
        try:
            lista = [obj['Key'] for obj in sorted(lista["Contents"], key=get_last_modified, reverse=True)]
        except:
            break
        # Ens quedem amb el primer fitxer de la llista que és el més antic
        aux2 = lista[0]
        id = aux2[8:]
        b = pickle.dumps([])

        # Variable auxiliar per guardar el result.json 'antic' per poder comparar-lo més endavant
        obj1 = ibm_cos.get_object(Bucket=BUCKET_NAME, Key=name3)['Body'].read()
        obj1des = json.loads(obj1)

        # Guardem un write_id al bucket, esborrem el p_write_id i guardem el id a la write_permission_list
        ibm_cos.put_object(Bucket=BUCKET_NAME, Key=name + id, Body=b)
        ibm_cos.delete_object(Bucket=BUCKET_NAME, Key=aux2)
        write_permission_list.append(id)

        # Monitoritzem el bucket per quan el result.json s'actualitzi, si les variables coincideixen fem un sleep, si no sortim
        while True:
            obj2 = ibm_cos.get_object(Bucket=BUCKET_NAME, Key=name3)['Body'].read()
            obj2des = json.loads(obj2)
            if obj1des != obj2des:
                break
            else:
                t.sleep(x)

        # Esborrem el write_id del bucket
        ibm_cos.delete_object(Bucket=BUCKET_NAME, Key=name + id)
        cont = cont + 1
    return write_permission_list


def slave(id, x, ibm_cos):
    b = pickle.dumps([])
    id = '{' + str(id) + '}'

    # Creem un p_write_id buit i el guardem al bucket
    ibm_cos.put_object(Bucket=BUCKET_NAME, Key=name2 + str(id), Body=b)

    # Monitoritzem el bucket fins a trobar un write_id que correspongui al seu id i sortim, mentre no en trobi fa sleep
    while True:
        try:
            obj = ibm_cos.get_object(Bucket=BUCKET_NAME, Key=name + str(id))
            break
        except:
            t.sleep(x)

    # Agafem el fitxer result.json i fem append del nou id i el tornem a guardar al bucket
    file = ibm_cos.get_object(Bucket=BUCKET_NAME, Key=name3)['Body'].read()
    dec = json.loads(file)
    dec.append(id)
    aux2 = json.dumps(dec)
    ibm_cos.put_object(Bucket=BUCKET_NAME, Key=name3, Body=aux2)

def delete_jobs(ibm_cos):
    lista_key = []
    lista = ibm_cos.list_objects(Bucket=BUCKET_NAME, Prefix=name4)
    for obj in lista['Contents']:
        lista_key.append(obj['Key'])
    for x in lista_key:
        ibm_cos.delete_object(Bucket=BUCKET_NAME, Key=x)



if __name__== '__main__':
    pw = pywren.ibm_cf_executor()
    ibm_cos = pw.internal_storage.get_client()

    # Esborrem el result.json antic per evitar conflictes i creem un de nou
    ibm_cos.delete_object(Bucket=BUCKET_NAME, Key=name3)
    cont = json.dumps(data)
    ibm_cos.put_object(Bucket=BUCKET_NAME, Key=name3, Body=cont)
    start_time = t.time()
    pw.map(slave, range(N_SLAVES))
    pw.call_async(master, time)
    write_permission_list = pw.get_result()
    result = ibm_cos.get_object(Bucket=BUCKET_NAME, Key=name3)['Body'].read()
    result = json.loads(result)
    elapsed_time = t.time() - start_time
    print("Time: {0:.2f} secs. ".format(elapsed_time))
    print(result)
    print(write_permission_list)
    # Comprovem si les llistes coincideixen
    if write_permission_list == result:
        print("Les llistes coincideixen.")
    else:
        print("Les llistes son diferents.")
    delete_jobs(ibm_cos)
from pymongo import MongoClient
import csv
from datetime import datetime
import time

cliente = MongoClient('localhost', 11017)

now = datetime.now()
timestamp = datetime.timestamp(now)

banco = cliente.admin

path_log = ''

time_out=0
with open(path_log+"/logMongo.csv", "a") as log:
    log.write('"timestamp";"asserts_regular";"asserts_warning";"asserts_msg";"asserts_user";"asserts_rollovers";"extra_info_page_faults";"connections_current";"connections_available";"currentQueue_total";"currentQueue_readers";"currentQueue_writers";"locks_database_acquireCount_r";"locks_database_acquireCount_w";"locks_collection_acquireCount_r";"locks_collection_acquireCount_w";"resul_timeOut"\n')
    while(True):
        resul = banco.command("serverStatus")

        asserts_regular=resul['asserts']['regular']
        asserts_warning=resul['asserts']['warning']
        asserts_msg=resul['asserts']['msg']
        asserts_user=resul['asserts']['user']
        asserts_rollovers=resul['asserts']['rollovers']

        extra_info_page_faults=resul['extra_info']['page_faults']

        connections_current = resul['connections']['current']
        connections_available = resul['connections']['available']

        currentQueue_total=resul['globalLock']['currentQueue']['total']
        currentQueue_readers=resul['globalLock']['currentQueue']['readers']
        currentQueue_writers=resul['globalLock']['currentQueue']['writers']

        locks_database_acquireCount_r=resul['locks']['Database']['acquireCount']['r']
        locks_database_acquireCount_w=resul['locks']['Database']['acquireCount']['w']

        locks_collection_acquireCount_r=resul['locks']['Collection']['acquireCount']['r']
        locks_collection_acquireCount_w=resul['locks']['Collection']['acquireCount']['w']
        
        metrics_cursor_timeOut=resul['metrics']['cursor']['timedOut']

        if time_out>0:
            resul_timeOut =int(metrics_cursor_timeOut)-time_out 
        else:
            resul_timeOut=0
            time_out=int(metrics_cursor_timeOut)

        log.write(f"{timestamp};{asserts_regular};{asserts_warning};{asserts_msg};{asserts_user};{asserts_rollovers};{extra_info_page_faults};{connections_current};{connections_available};{currentQueue_total};{currentQueue_readers};{currentQueue_writers};{locks_database_acquireCount_r};{locks_database_acquireCount_w};{locks_collection_acquireCount_r};{locks_collection_acquireCount_w};{resul_timeOut}")
        
        time.sleep(300)
        pass




#parse the parameter to connect to mongo database
import configparser
import urllib.parse
confg = configparser.ConfigParser()
import os,sys
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
print("Ubicación actual del script:", os.path.abspath(__file__))
confg.read_file(open('../conf/conf.conf'))


def get_postres_conn_str():
    # check the connection to the database
    p_pass = confg.get('db_conf', 'postgres_pass')
    p_user = confg.get('db_conf', 'postgres_user')
    p_port = confg.get('db_conf', 'postgres_port')
    p_host = confg.get('db_conf', 'postgres_db_host')
    p_db = confg.get('db_conf', 'potgres_db_name')

    p_pass_encoded = urllib.parse.quote(p_pass)

    host_2 = f"user={p_user} password={p_pass_encoded} dbname={p_db} host={p_host} port={p_port}"
    
    return host_2
print(get_postres_conn_str())
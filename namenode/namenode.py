import psycopg2
import threading
import socket
import os

datanode1_ip = "localhost"
datanode1_port = 4000
datanode2_ip = "localhost"
datanode2_port = 3001

BUFFER_SIZE = 1024

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)


def make_query(query):
    conn = psycopg2.connect(dbname='postgres', user='postgres',
                            password='postgres', host='localhost', port="5432")
    cursor = conn.cursor()
    print(conn)
    cursor.execute(query)
    conn.commit()
    res = cursor.fetchall()
    cursor.close()
    conn.close()
    return res


def initialize():
    make_query('CREATE TABLE files (filename Text, datanode1 TEXT, datanode2 TEXT);')

def send_msg(ip,port, msg):
    s.connect((ip, port))
    s.send(msg)
    # data = s.recv(BUFFER_SIZE)
    s.close()

def get_ips():
    # TODO
    return ((datanode1_ip, datanode1_port), (datanode2_ip,datanode2_port))

def send_file(filename, ip, port):
    os.system("scp ")

def send_new_file(filename):
    os.mknod(filename)
    send_file(filename)


def file_create(filename):
    ips = get_ips()
    send_msg(ips[0][0], ips[0][1], "send_file")
    send_msg(ips[1][0], ips[1][1], "send_file")
    make_query("Insert into files(filename, datanode1, datadone2) VALUES ({},{},{})".format(filename, ips[0][0], ips[1][1]))
    send_new_file(filename)


if __name__=="__main__":
    while True:
        a = input()
        if a == "initialize":
            initialize()
            send_msg(datanode1_ip, datanode1_port, a)
            send_msg(datanode2_ip, datanode2_port, a)

import psycopg2
import threading
import socket

datanode1_ip = "localhost"
datanode1_port = 4000
datanode2_ip = "localhost"
datanode2_port = 3001

BUFFER_SIZE = 1024


def initialize():
    conn = psycopg2.connect(dbname='postgres', user='postgres',
                            password='postgres', host='localhost', port="5432")
    cursor = conn.cursor()
    print(conn)
    cursor.execute('CREATE TABLE files (filename Text, datanode1 TEXT, datanode2 TEXT);')
    conn.commit()
    cursor.close()
    conn.close()

    send_msg(datanode1_ip, "msg")
    send_msg(datanode2_ip, "msg")

def send_msg(ip,port, msg):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((ip, port))
    s.send(msg)
    data = s.recv(BUFFER_SIZE)
    s.close()


if __name__=="__main__":
    while True:
        a = input()
        if a == "initialize":
            initialize()
            send_msg(datanode1_ip, datanode1_port, a)

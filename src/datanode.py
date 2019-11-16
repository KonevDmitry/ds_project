import os
import shutil
import random
import time
from threading import Thread
import socket
out = "./var/storage"

TCP_IP = '127.0.0.1'
TCP_PORT = 5005
BUFFER_SIZE = 1024

def initial_command():
    if not os.path.exists(out):
        os.makedirs(out)
    else:
        shutil.rmtree(out)
        os.makedirs(out)

def get_socket():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print(s)
    s.bind((TCP_IP, TCP_PORT))
    s.listen(1)
    conn, addr = s.accept()
    print('Connection address:', addr)
    while 1:
        data = conn.recv(BUFFER_SIZE)
        if not data:
            break
    print("received data:", data)
    conn.send(data)  # echo
    conn.close()


if __name__ == '__main__':
    get_socket()
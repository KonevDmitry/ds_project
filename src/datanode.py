import os
import shutil
import random
import time
from threading import Thread
import socket

out = "./var/storage"

TCP_IP = '127.0.0.1'
TCP_PORT = 3002
BUFFER_SIZE = 1024


def initial_command():
    if not os.path.exists(out):
        os.makedirs(out)
    else:
        shutil.rmtree(out)
        os.makedirs(out)


def command(command):
    if (command == "init"):
        initial_command()
    elif (command == "create"):
        print("created")
    elif (command == "read"):
        print("readddd")
    elif (command == "write"):
        print("xz ")


def init():
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
    name = ""
    init()

import os
import shutil
import random
import time
from threading import Thread
import socket

out = "./var/storage"

TCP_IP = "10.91.55.114"
TCP_PORT = 4000
BUFFER_SIZE = 1024


def initialization():
    if not os.path.exists(out):
        os.makedirs(out)
    else:
        shutil.rmtree(out)
        os.makedirs(out)


# переделать
def read_file(name):
    with open(out + "/" + name, 'rb') as file:
        return file.read()
    """client side
    with open('./received_files/' + fileName, 'wb') as handle:
  handle.write(data)"""


def copy_file(path1, path2):
    shutil.copyfile(src, dst)


def move_file(path1, path2):
    os.rename(path1, path2)
    shutil.move(path1, path2)
    os.replace(path1, path2)


def delete_directory(path):
    return shutil.rmtree(path)


def make_directory(path):
    return os.makedirs(path)


def read_directory(path):
    return os.listdir(path)


def command(command):
    if (command == "initialize"):
        initialization()
    elif (command == "create"):
        pass
    elif (command == "read"):
        pass
    elif (command == "write"):
        print("xz ")
    elif (command == "send_file"):
        pass


def get_message():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((TCP_IP, TCP_PORT))
    s.listen(5)
    conn, addr = s.accept()
    print('Connection address:', addr)
    while 1:
        data = conn.recv(BUFFER_SIZE)
        if not data: break
        print("received data:", data)
        conn.send(data)  # echo
        command(data)
    conn.close()


if __name__ == '__main__':
    name = ""
    get_message()

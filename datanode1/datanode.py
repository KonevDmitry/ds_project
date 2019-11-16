import os
import shutil
import socket
out = "./var/storage"

TCP_IP = "10.91.8.155"
TCP_PORT = 3001
BUFFER_SIZE = 1024


def initial_command():
    if not os.path.exists(out):
        os.makedirs(out)
    else:
        shutil.rmtree(out)
        os.makedirs(out)


def create_file(name):
    os.mknod(out + '/' + name)


def command(command):
    data = command.split(' ')
    if data[0] == "init":
        initial_command()
    elif data[0] == "create":
        create_file(data[1])
        print("created")
    elif data[0] == "read":
        print("read")
    elif data[0] == "write":
        print("xz ")


def get_message():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((TCP_IP, TCP_PORT))
    s.listen(5)
    conn, addr = s.accept()
    print('Connection address:', addr)
    while 1:
        data = conn.recv(BUFFER_SIZE)
        if not data:
            break
        print("received data:", data)
        command(data)
        conn.send(data)

    conn.close()


if __name__ == '__main__':
    name = ""
    get_message()

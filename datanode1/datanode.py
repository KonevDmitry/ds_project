import os
import shutil
import socket

out = "./var/storage"
TCP_IP = "10.91.8.155"
TCP_PORT = 3001
BUFFER_SIZE = 1024


def initialization():
    if not os.path.exists(out):
        os.makedirs(out)
    else:
        shutil.rmtree(out)
        os.makedirs(out)


def read_file(path, s1):
    # host = socket.gethostname()  # Get local machine name
    # port = 12345  # Reserve a port for your service.
    f = open(path, 'rb')
    l = f.read(1024)
    while (l):
        print(l)
        s1.send(l)
        l = f.read(1024)
    s1.send(b'')
    f.close()


def delete_file(path):
    os.remove(path)


def copy_file(path1, path2):
    shutil.copyfile(path1, path2)


def move_file(path1, path2):
    shutil.move(path1, path2)


def delete_directory(path):
    return shutil.rmtree(path)


def make_directory(path):
    return os.makedirs(path)


def create_file(path):
    return os.mknod(path)


def command(command, s1):
    data = command.decode().split(' ')
    if data[0] == "init":
        initialization()
    if data[0] == "create":
        create_file(data[1])
    elif data[0] == "copy":
        copy_file(data[1], data[2])
    elif data[0] == "move":
        move_file(data[1], data[2])
    elif data[0] == "delete":
        delete_file(data[1])
    elif data[0] == "makedir":
        make_directory(data[1])
    elif data[0] == "deletedir":
        delete_directory(data[1])
    elif data[0] == "read":  # !!!!!!
        read_file(data[1], s1)


def get_message():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((TCP_IP, TCP_PORT))
    s.listen(5)

    s1=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s1.connect(("10.91.8.168", 3002))

    conn, addr = s.accept()
    print('Connection address:', addr)
    while 1:
        data = conn.recv(BUFFER_SIZE)
        if data == b'close':
            conn.close()
            s1.close()
            s.close()
            break

        print("received data:", data)
        command(data, s1)
    conn.close()


if __name__ == '__main__':
    name = ""
    get_message()

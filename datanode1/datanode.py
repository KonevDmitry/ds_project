import os
import shutil
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
    """
    client side
    with open('./received_files/' + fileName, 'wb') as handle:
    handle.write(data)
    """


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


def command(command):
    data = command.split(' ')
    if data[0] == "init":
        initialization()
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
        read_file(data[1])


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
    command("move ./var/storage/new3/1 ./var/storage/new1/1")
    # get_message()

import psycopg2
import threading
import socket
import os
import random
import time
import pathlib

var_stor = "./var/storage"
curr_dir = "/"

# , "10.91.8.155:3001"
datanodes = ["10.91.8.155:3000"]
TCP_IP = '10.91.8.168'
TCP_PORT = 3002
rec_s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
rec_s.bind((TCP_IP, TCP_PORT))
rec_s.listen(5)
BUFFER_SIZE = 1024
s = {}
conn = {}

for i in datanodes:
    r = i.split(":")
    conn[i], addr = rec_s.accept()
    print(addr)
    s[i] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s[i].connect((r[0], int(r[1])))


def check_file(filename):
    if curr_dir != '/':
        path = curr_dir + "/" + filename
    else:
        path = curr_dir + filename
    return check_file_path(path)


def check_file_path(path):
    l = len(make_query("SELECT * From files where filename='{}'".format(path), True))
    if l == 0:
        print("Not found:'{}'".format(path))
        return False
    return True


def check_exists(filename):
    if curr_dir != '/':
        path = curr_dir + "/" + filename
    else:
        path = curr_dir + filename
    return check_exists_path(path)


def check_exists_path(path):
    l = len(make_query("SELECT * From files where filename='{}'".format(path), True))
    print(l)
    if l != 0:
        print("Already Exists:'{}'".format(path))
        return False
    return True


def get_ips_for_file(path):
    a = make_query("SELECT * FROM files WHERE filename='{}'".format(path), True)
    return a[0][1], a[0][2]


def make_query(query, is_return):
    conn = psycopg2.connect(dbname='postgres', user='postgres',
                            password='postgres', host='localhost', port="5432")
    cursor = conn.cursor()
    print(conn)
    cursor.execute(query)
    conn.commit()
    if is_return:
        res = cursor.fetchall()
    cursor.close()
    conn.close()
    if is_return:
        return res


def initialize():
    make_query("DROP TABLE IF EXISTS files;", is_return=False)
    make_query('CREATE TABLE files (filename Text, datanode1 TEXT, datanode2 TEXT, dir TEXT, is_dir BOOLEAN);', False)
    for i in s.values():
        send_msg(i, "init")


def send_msg(sock, msg):
    sock.send(bytes(msg, "utf-8"))


def get_ips():
    count = random.sample(datanodes, 1) # TODO сменть на 2
    return count


def send_file(file, iport):

    return #TODO


def check_nodes():
    while True:
        for i in datanodes:
            a = i.split(":")
            response = os.system("ping -c 1 " + a[0])
            if response == 0:
                backup(i)
        time.sleep(5)


def backup(addr):
    a = make_query("SELECT * FROM files Where datanode1='{}' AND is_dir=FALSE;".format(addr), True)
    a += make_query("SELECT * FROM files Where datanode2='{}' AND is_dir=FALSE;".format(addr), True)
    for i in a:
        if i[1] != addr:
            file = read_from_node(i[0], i[1])  # TODO
        else:
            file = read_from_node(i[0], i[2])
        write1(i, file)


def read(filename):
    if check_file(filename):
        ips = get_ips_for_file(curr_dir + '/' + filename)
        read_from_node(filename, ips[0])


def read_from_node(filename, addr):
    if check_file(filename):
        if curr_dir != "/":
            path = curr_dir + "/" + filename
        else:
            path = curr_dir + filename

        sock = s[addr]
        rec_con = conn[addr]
        send_msg(sock, "read " + var_stor + path)
        pathlib.Path('./received_files').mkdir(parents=True, exist_ok=True)
        with open('./received_files/' + filename, 'wb') as handle:
            time.sleep(2)
            l = rec_con.recv(1024)
            handle.write(l)

            while (len(l) > 1024):
                print("Receiving...")
                l = rec_con.recv(1024)
                handle.write(l)
                print(l)
            handle.close()

def write(path, dfs_path):
    ips = get_ips()
    for i in ips:
        send_file(path, dfs_path, i)

def send_file(path, dfs_path, ip):
    # host = socket.gethostname()  # Get local machine name
    # port = 12345  # Reserve a port for your service.
    s1 = s[ip]
    send_msg(s1, "write {}".format(var_stor+dfs_path))
    f = open(path, 'rb')
    l = f.read(1024)
    while (l):
        print(l)
        s1.send(l)
        l = f.read(1024)
    s1.send(b'')
    f.close()


def write1(fileinf, file):
    # TODO
    if len(datanodes) < 3:
        print("Can't replicate")
    else:
        for i in datanodes:
            if i != fileinf[1] and i != fileinf[2]:
                send_file(file, i)


def mkdir(new_path):
    if check_exists(new_path):
        if curr_dir != "/":
            path = curr_dir + "/" + new_path
        else:
            path = curr_dir + new_path
        make_query("Insert into files(filename, datanode1, datanode2, dir, is_dir) VALUES ('{0}','{1}','{2}','{3}', {4})"
                   .format(path, "_", "_", curr_dir, True), False)
        for i in s.values():
            send_msg(i, "makedir " + var_stor + path)


# def test_file(filename):
#     if curr_dir != "/":
#         make_query("Insert into files(filename, datanode1, datanode2, dir, is_dir) VALUES ('{0}','{1}','{2}','{3}', {4})"
#                    .format(curr_dir+"/"+filename, "12", "ips[1][0]", curr_dir, False), False)
#     else:
#         make_query("Insert into files(filename, datanode1, datanode2, dir, is_dir) VALUES ('{0}','{1}','{2}','{3}', {4})"
#                    .format(curr_dir+filename, "12", "ips[1][0]", curr_dir, False), False)
#


def info(filename):
    if check_file(filename):
        if curr_dir != "/":
            return make_query("SELECT * FROM files WHERE filename = '{0}';".format(curr_dir + "/" + filename), True)
        else:
            return make_query("SELECT * FROM files WHERE filename = '{0}';".format(curr_dir + filename), True)


def delete_dir(folder):
    if check_file(folder):
        if curr_dir != "/":
            path1 = curr_dir + "/" + folder  # /abc => /abc/1488
        else:
            path1 = curr_dir + folder  # / => /abc
        a = make_query("SELECT * FROM files WHERE dir='{}'".format(path1), True)
        path = var_stor + path1

        if len(a) == 0:
            for i in s.values():
                send_msg(i, "deletedir " + path)
                make_query("DELETE FROM files where filename='{}'".format(path1), False)

        else:
            print("The folder is not empty. Are you sure you want to delete it? [y/n]\n>")
            a = input()
            if a == 'y':
                for i in s.values():
                    send_msg(i, "deletedir " + path)
                    make_query("DELETE FROM files where dir='{}'".format(path1), False)
                    make_query("DELETE FROM files where filename='{}'".format(path), False)


def delete_file(filename):
    if check_file(filename):
        if curr_dir != "/":
            path1 = curr_dir + "/" + filename
        else:
            path1 = curr_dir + filename
        a = make_query("SELECT * FROM files WHERE filename='{}'".format(path1), True)
        path = var_stor + path1
        if len(a) == 0:
            print("File doesn't exist")
            return
        else:
            make_query("DELETE FROM files Where filename='{}'".format(path1), False)
            send_msg(s[a[0][1]], "delete {}".format(path))
            # send_msg(s[a[0][2]], "delete {}".format(path)) # TODO анеоментнуть


def close():
    rec_s.close()
    rec_s.detach()
    for i in datanodes:
        send_msg(s[i], "close")
        s[i].close()
        conn[i].close()


def create(filename):
    if check_exists(filename):
        ips = get_ips()
        send_msg(s[ips[0]], "create " + var_stor + curr_dir + filename)
        # send_msg(s[ips[1]], "create " + var_stor+curr_dir+filename) #TODO
        if curr_dir != "/":
            make_query(
                "Insert into files(filename, datanode1, datanode2, dir, is_dir) VALUES ('{0}','{1}','{2}','{3}', {4})"
                    .format(curr_dir + "/" + filename, ips[0], "ips[1]", curr_dir, False), False)
        else:
            make_query(
                "Insert into files(filename, datanode1, datanode2, dir, is_dir) VALUES ('{0}','{1}','{2}','{3}', {4})"
                    .format(curr_dir + filename, ips[0], "ips[1]", curr_dir, False), False)


def move(path1, path2):
    if check_file_path(path1):
        ips = get_ips_for_file(path1)
        make_query(
            "Insert into files(filename, datanode1, datanode2, dir, is_dir) VALUES ('{0}','{1}','{2}','{3}', {4})"
                .format(path2, ips[0], "ips[1]", curr_dir, False), False)
        make_query("DELETE FROM files Where filename='{}'".format(path1), False)
        print(ips)
        send_msg(s[ips[0]], "move " + var_stor+path1 + " " + var_stor+path2)
        # send_msg(s[ips[1]], "move {} {}".format(path1, path2))


def copy(path1, path2):
    if check_file_path(path1) and check_exists_path(path2):
        ips = get_ips_for_file(path1)
        make_query(
            "Insert into files(filename, datanode1, datanode2, dir, is_dir) VALUES ('{0}','{1}','{2}','{3}', {4})"
                .format(path2, ips[0], "ips[1]", curr_dir, False), False)
        send_msg(s[ips[0]], "copy {} {}".format(path1, path2))
        # send_msg(s[ips[1]], "move {} {}".format(path1, path2))


def ls():
    l = make_query("SELECT * FROM files WHERE dir = '{0}';".format(curr_dir), True)
    for j in l:
        if j[4]:
            print("Directory {}".format(j[0]))
        else:
            print("File {}".format(j[0]))


def cd(path):
    global curr_dir
    if path == "/" or check_file(path):
        print(path)
        if path == "/":
            curr_dir = "/"  # => /abadsa/asjkdh => /
        else:
            if curr_dir == "/":
                curr_dir = curr_dir + path  # / => /abc
            else:
                curr_dir = curr_dir + "/" + path  # /


if __name__ == "__main__":
    # t = threading.Thread(target=check_nodes())
    # t.daemon = True
    # t.start()
    while True:
        print(curr_dir + ">", end=" ")
        a = input().split(" ")
        if a[0] == "initialize":
            initialize()

        elif a[0] == "cd":
            cd(a[1])
        elif a[0] == "ls":
            ls()

        elif a[0] == "info":
            print(info(a[1])[0])

        elif a[0] == "mkdir":
            mkdir(a[1])
            ls()

        elif a[0] == 'read':
            read_from_node(a[1], "10.91.8.155:3001")  # TODO

        elif a[0] == "deletedir":
            delete_dir(a[1])
            ls()
        elif a[0] == "close":
            close()
            exit(0)
        elif a[0] == "delete":
            delete_file(a[1])
        elif a[0] == 'create':
            create(a[1])

        elif a[0] == 'move':
            move(a[1], a[2])

        elif a[0] == 'copy':
            copy(a[1], a[2])

        elif a[0] == 'write':
            write(a[1], a[2])

        else:
            print(a[0] + ": Command not found")

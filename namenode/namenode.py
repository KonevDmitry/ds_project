import psycopg2
import threading
import socket
import os
import random
import time
import pathlib
import sys
import subprocess
import argparse
import traceback

parser = argparse.ArgumentParser()
parser.add_argument("ip")
parser.add_argument("port")
args = parser.parse_args()

TCP_IP = args.ip
TCP_PORT = int(args.port)

var_stor = "/var/storage"
curr_dir = "/"


datanodes = []

rec_s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
rec_s.bind((TCP_IP, TCP_PORT))
rec_s.listen(5)
BUFFER_SIZE = 1024
s = {}
conn = {}
n_repl = 2
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
    make_query('CREATE TABLE files (filename Text, datanode1 TEXT, datanode TEXT, dir TEXT, is_dir BOOLEAN, size TEXT);', False)
    for i in s.values():
        send_msg(i, "init")


def send_msg(sock, msg):
    sock.send(bytes(msg, "utf-8"))


def get_ips():
    count = random.sample(datanodes, 2)
    return count


def check_nodes():
    while True:
        for i in datanodes:
            a = i.split(":")
            response = subprocess.getstatusoutput("ping -c 1 " + a[0])
            if response[0] == 1:
                print("Node {} stopped working, starting backup process".format(a))
                backup(i)
        time.sleep(5)


def manage_connections():
    while 1:
        connection, addr = rec_s.accept()
        port = connection.recvfrom(1024)
        print("Node with address: {} connected". format(addr))
        key = addr[0] + ":" + port[0].decode()
        s[key] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s[key].connect((addr[0], int(port[0].decode())))
        conn[key] = connection
        datanodes.append(key)
        create_dirs(key)
        time.sleep(3)


def create_dirs(addr):
    a = make_query("SELECT * from files where is_dir=TRUE;", True)

    send_msg(sock=s[addr], msg="init")
    for i in a:
        if len(i)>0:
            mkdir2(addr, i[0])

def backup(addr):
    a = make_query("SELECT * FROM files Where datanode1='{}' AND is_dir=FALSE;".format(addr), True)
    a += make_query("SELECT * FROM files Where datanode='{}' AND is_dir=FALSE;".format(addr), True)
    back_dir = '/backup_{}'.format(addr)
    for i in a:
        if i[1] != addr:
            read_for_backup(i[0], i[1], back_dir)  # TODO
        else:
            read_for_backup(i[0], i[2], back_dir)
        write1(i, i[0], addr, back_dir)
    datanodes.remove(addr)
    print("Finished backup of the node")


def read(filename):
    if check_file(filename):
        if curr_dir == "/":
            ips = get_ips_for_file(curr_dir + filename)
        else:
            ips = get_ips_for_file(curr_dir + '/' + filename)
        read_from_node(filename, ips[0], '/received_files')


def read_for_backup(filename, addr, save_dir):
    sock = s[addr]
    rec_con = conn[addr]
    send_msg(sock, "read " + var_stor + filename)
    pathlib.Path(save_dir+filename[:filename.rfind("/")+1]).mkdir(parents=True, exist_ok=True)
    with open(save_dir + filename, 'wb') as handle:
        l = rec_con.recv(1024)
        if l == b'1':
            l = rec_con.recv(1024)
            handle.write(l)
            print("Ok")
            while (len(l) > 1024):
                print("Receiving...")
                l = rec_con.recv(1024)
                handle.write(l)
                print(l)
            handle.close()
        else:
            handle.close()

def read_from_node(filename, addr, save_dir):
    if check_file(filename):
        if curr_dir != "/":
            path = curr_dir + "/" + filename
        else:
            path = curr_dir + filename

        sock = s[addr]
        rec_con = conn[addr]
        send_msg(sock, "read " + var_stor + path)
        pathlib.Path(save_dir+path[:path.rfind("/")+1]).mkdir(parents=True, exist_ok=True)
        with open(save_dir + path, 'wb') as handle:
            if rec_con.recv(1024) == b'1':
                l = rec_con.recv(1024)
                handle.write(l)
                print("Ok")
                while (len(l) > 1024):
                    print("Receiving...")
                    l = rec_con.recv(1024)
                    handle.write(l)
                    print(l)
                handle.close()
            else:
                open('/received_files/' + filename, 'w+').close()

def write(path, dfs_path):
    ips = get_ips()
    if len(dfs_path[:dfs_path.rfind('/')]) == 0:
        make_query("INSERT INTO files(filename, datanode1, datanode, dir,is_dir, size) VALUES ('{}','{}','{}', '{}',{}, '{}')".
               format(dfs_path, ips[0], ips[1], '/',False, os.path.getsize(path)), False)
    else:
        make_query("INSERT INTO files(filename, datanode1, datanode, dir,is_dir, size) VALUES ('{}','{}','{}', '{}',{}, '{}')".
               format(dfs_path, ips[0], ips[1], dfs_path[:dfs_path.rfind('/')],False, os.path.getsize(path)), False)
    for i in ips:
        send_file(path, dfs_path, i)


def send_file(path, dfs_path, ip):
    s1 = s[ip]
    send_msg(s1, "write {}".format(var_stor+dfs_path))
    time.sleep(2)
    f = open(path, 'rb')
    l = f.read(1024)
    while (l):
        print(l)
        s1.send(l)
        l = f.read(1024)
    time.sleep(2)
    s1.send(b'0')
    f.close()


def write1(fileinf, file, addr, backup_dir):
    if len(datanodes) <= n_repl:
        print("Can't replicate")
    else:
        for i in datanodes:
            if i != fileinf[1] and i != fileinf[2]:
                send_file(backup_dir + file, file, i)
                if addr == fileinf[1]:
                    make_query("UPDATE files set datanode1='{}'".format(i), False)
                else:
                    make_query("UPDATE files set datanode='{}'".format(i), False)


def mkdir(new_path):
    if check_exists(new_path):
        if curr_dir != "/":
            path = curr_dir + "/" + new_path
        else:
            path = curr_dir + new_path
        make_query("Insert into files(filename, datanode1, datanode, dir, is_dir, size) VALUES ('{0}','{1}','{2}','{3}', {4}, '-')"
                   .format(path, "_", "_", curr_dir, True), False)
        for i in s.values():
            send_msg(i, "makedir " + var_stor + path)

def mkdir2(addr, path):
    send_msg(s[addr], "makedir " + var_stor + path)


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
                make_query("DELETE FROM files where filename='{}'".format(path1), False)


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
            send_msg(s[a[0][2]], "delete {}".format(path))


def close():
    for i in datanodes:
        send_msg(s[i], "close")
        s[i].close()
        conn[i].close()
    rec_s.close()
    rec_s.detach()

def create(filename):
    if check_exists(filename):
        ips = get_ips()
        if curr_dir != "/":
            send_msg(s[ips[0]], "create " + var_stor+curr_dir + "/" + filename)
            send_msg(s[ips[1]], "create " + var_stor+curr_dir + "/" + filename) #TODO
            make_query(
                "Insert into files(filename, datanode1, datanode, dir, is_dir, size) VALUES ('{0}','{1}','{2}','{3}', {4}, '0')"
                    .format(curr_dir + "/" + filename, ips[0], ips[1], curr_dir, False), False)
        else:
            send_msg(s[ips[0]], "create " + var_stor + curr_dir + filename)
            send_msg(s[ips[1]], "create " + var_stor + curr_dir + filename) #TODO
            make_query(
                "Insert into files(filename, datanode1, datanode, dir, is_dir, size) VALUES ('{0}','{1}','{2}','{3}', {4}, '0')"
                    .format(curr_dir + filename, ips[0], ips[1], curr_dir, False), False)


def move(path1, path2):
    if check_file_path(path1):
        ips = get_ips_for_file(path1)
        make_query(
            "Insert into files(filename, datanode1, datanode, dir, is_dir) VALUES ('{0}','{1}','{2}','{3}', {4})"
                .format(path2, ips[0], ips[1], path2[:path2.rfind('/')], False), False)
        make_query("DELETE FROM files Where filename='{}'".format(path1), False)
        print(ips)
        send_msg(s[ips[0]], "move " + var_stor+path1 + " " + var_stor+path2)
        send_msg(s[ips[1]], "move " + var_stor+path1 + " " + var_stor+path2)


def copy(path1, path2):
    if check_file_path(path1) and check_exists_path(path2):
        ips = get_ips_for_file(path1)
        make_query(
            "Insert into files(filename, datanode1, datanode, dir, is_dir) VALUES ('{0}','{1}','{2}','{3}', {4})"
                .format(path2, ips[0], ips[1], path2[:path2.rfind('/')], False), False)
        send_msg(s[ips[0]], "copy {} {}".format(var_stor+path1, var_stor+path2))
        send_msg(s[ips[1]], "copy {} {}".format(var_stor+path1, var_stor+path2))


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
    make_query("CREATE TABLE IF NOT EXISTS files (filename Text, datanode1 TEXT, datanode TEXT, dir TEXT, is_dir BOOLEAN, size TEXT);", False)
    t = threading.Thread(target=manage_connections)
    t.daemon = True
    t.start()
    t2 = threading.Thread(target=check_nodes)
    t2.daemon = True
    t2.start()
    count = 0
    while len(datanodes) < n_repl:
        print("Waiting for datanodes")
        count +=1
        count = min(count, 15)
        time.sleep(count)
    while True:
        try:
            print(curr_dir + ">", end=" ")
            b = input()
            a = b.split(" ")
            if a[0] == "init":
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
                read(a[1])
            elif a[0] == "deletedir":
                delete_dir(a[1])
                ls()
            elif a[0] == "close":
                close()
                sys.exit(0)
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
            elif a[0] == 'lsdn':
                print(datanodes)
            else:
                print(a[0] + ": Command not found")
        except SystemExit:
            print("Bye")
            sys.exit(0)
        except KeyboardInterrupt:
            print("Bye")
            sys.exit(0)
        except:
            print("Something went wrong")
            continue
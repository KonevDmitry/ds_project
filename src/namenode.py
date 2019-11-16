import psycopg2
import threading
import socket
import os
import random

curr_dir = "/"
# , "10.91.8.155:3001"
datanodes = ["10.91.55.114:4000"]
BUFFER_SIZE = 1024
s = {}
for i in datanodes:
    r = i.split(":")
    s[r[0]] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s[r[0]].connect((r[0], int(r[1])))


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
    count = random.sample(datanodes,2)
    return count[0], count[1]


def send_file(filename, ip, port):
    os.system("scp -P {} {} datanode@{}:/var/storage/{}".format(port, filename, ip, filename))


def send_new_file(filename, ip1, ip2):
    os.mknod(filename)
    i1 = ip1.split(":")
    i2 = ip2.split(":")
    send_file(filename, i1[0], i1[1])
    send_file(filename, i2[0], i2[1])


def close_connections():
    for i in s.values():
        i.close()


def file_create(filename):
    ips = get_ips()
    if curr_dir != "/":
        make_query("Insert into files(filename, datanode1, datanode2, dir, is_dir) VALUES ('{0}','{1}','{2}','{3}', {4})"
                   .format(curr_dir+"/"+filename, ips[0], ips[1], curr_dir, False), False)
    else:
        make_query("Insert into files(filename, datanode1, datanode2, dir, is_dir) VALUES ('{0}','{1}','{2}','{3}', {4})"
                   .format(filename, ips[0], ips[1], curr_dir, False), False)
    send_new_file(filename, ips[0], ips[1])


def mkdir(new_path):
    if curr_dir != "/":
        path = curr_dir+"/"+new_path
    else:
        path = new_path
    make_query("Insert into files(filename, datanode1, datanode2, dir, is_dir) VALUES ('{0}','{1}','{2}','{3}', {4})"
               .format(path, "_", "_", curr_dir, True), False)
    for i in s.values():
        send_msg(i, "makedir "+"./var/storage/"+path)


def test_file(filename):
    if curr_dir != "/":
        make_query("Insert into files(filename, datanode1, datanode2, dir, is_dir) VALUES ('{0}','{1}','{2}','{3}', {4})"
                   .format(curr_dir+"/"+filename, "12", "ips[1][0]", curr_dir, False), False)
    else:
        make_query("Insert into files(filename, datanode1, datanode2, dir, is_dir) VALUES ('{0}','{1}','{2}','{3}', {4})"
                   .format(filename, "12", "ips[1][0]", curr_dir, False), False)


def ls():
    return make_query("SELECT * FROM files WHERE dir = '{0}';".format(curr_dir), True)


def info(filename):
    if curr_dir != "/":
        return make_query("SELECT * FROM files WHERE filename = '{0}';".format(curr_dir+"/"+filename), True)
    else:
        return make_query("SELECT * FROM files WHERE filename = '{0}';".format(filename), True)



if __name__=="__main__":
    while True:
        a = input()
        if a == "initialize":
            initialize()
        if a == "touch":
            print("Input name of the file:")
            filename = input()
            test_file(filename)

        if a == "cd":
            a = input()
            if a == "/":
                curr_dir = "/"
            else:
                curr_dir = curr_dir + a
            print(curr_dir)
            print(ls())

        if a == "info":
            a = input()
            print(info(a)[0])

        if a == "mkdir":
            print("Enter the directory name")
            a = input()
            mkdir(a)
            print(ls())
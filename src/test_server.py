import socket

if __name__ == '__main__':

    TCP_IP = "10.91.55.114"
    TCP_PORT = 80
    BUFFER_SIZE = 1024  # Normally 1024, but we want fast response

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #s.connect((TCP_IP,TCP_PORT))
    s.bind((TCP_IP, TCP_PORT))
    s.listen(5)
    conn, addr = s.accept()
    print('Connection address:', addr)
    while 1:
        data = conn.recv(BUFFER_SIZE)
        if not data: break
        print("received data:", data)
        conn.send(data)  # echo
    conn.close()

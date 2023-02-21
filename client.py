import socket
import re
import json
import os
import threading

HEADER = 64
PORT = 5051
PORT2 = 0
FORMAT = 'utf-8'
DISCONNECT_MESSAGE = "!DISCONNECT"
SERVER = "127.0.0.1"
ADDR = (SERVER, PORT)
ADDR2 = (SERVER,PORT2)

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(ADDR)
worker2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
worker2.bind(ADDR2)

def send(msg):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    client.send(send_length)
    client.send(message)
    res = client.recv(2048).decode(FORMAT)
    print(res)
    return res


def handle_client(conn, addr):
    print(f"[NEW CONNECTION] {addr} connected.")

    connected = True
    try:
        while connected:
            msg_length = conn.recv(HEADER).decode(FORMAT)
            if msg_length:
                msg_length = int(msg_length)
                msg = conn.recv(msg_length).decode(FORMAT)
                if msg == DISCONNECT_MESSAGE:
                    connected = False

                print(f"[{addr}] {msg}")

                if msg.startswith('log:'):
                    msg = msg.replace('log: ','')
                    msg = json.loads(msg)

                    json_formatted_str = json.dumps(msg, indent=2)

                    print(json_formatted_str)
                conn.send("got".encode(FORMAT))

    except Exception as ex:
        print("connection closed unexpectedly")
        print(ex)
    conn.close()


def start():
    worker2.listen()
    print(f"[LISTENING] Client is listening on {SERVER}")
    while True:
        conn, addr = worker2.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()
        print(f"[ACTIVE CONNECTIONS] {threading.activeCount() - 1}")

def sendCommand():
    global client,DISCONNECT_MESSAGE
    command = input()
    while(command!='-1'):
        if command.startswith('wlf job add'):
            if command.__contains__('-simple'):
                pattern = '(\{.*\})'
                result = re.findall(pattern,command)
                if(len(result)):
                    send('simple: '+result[0])
                else:
                    print("No command found")
            elif command.__contains__('-executable'):
                pattern = '(\{.*\}) -executable \{(.*)\}'
                result = re.findall(pattern,command)
                if(len(result)):
                    file_name= result[0][1]
                    if not (os.path.exists(file_name)):
                        print("fiel not found")
                    else:
                        file_size = os.path.getsize(file_name)
                        x = {
                                "cmd": result[0][0],
                                "name": file_name.split('/')[-1],
                                "size": file_size
                                }
                        y = json.dumps(x)
                        res = send('executable: '+str(y))
                        if  res == "ready": # if server ready send file
                                    # Opening file and sending data.
                            with open(file_name, "rb") as file:
                                c = 0
                                        # Running loop while c != file_size.
                                while c <= file_size:
                                    data = file.read(1024)
                                    if not (data):
                                        break
                                    client.sendall(data)
                                    c += len(data)
                        
                        client.recv(2048).decode(FORMAT)
                else:
                    print("No command found")
                    
        elif command.startswith('wlf job list') or command.startswith('wlf node list') or command.startswith('wlf job logs') or command.startswith('wlf node top'):
            result = send(command)

            json_object = json.loads(result)

            json_formatted_str = json.dumps(json_object, indent=2)

            print(json_formatted_str)
        else:
            print('Please input in a valid format')

        command = input()

    send(DISCONNECT_MESSAGE)

if __name__ == '__main__':
    
    print("[STARTING] client is starting...")
    try:
        server = threading.Thread(target=start)
        server.start()
        

        config =  'This is Client: {"server":'+json.dumps(worker2.getsockname()[0])+', "port":'+str(worker2.getsockname()[1])+', "username":"admin"'+', "password":"admin"}'
        masterResponse =  send(config)
        if masterResponse=="OK":
            print("For ending connection please input -1")
            recieveClient = threading.Thread(target=sendCommand)
            recieveClient.start()

        elif masterResponse=="Unautorized":
            server.join()
            worker2.close()
            client.close()
            print("username and password were wrong")
        else:
            server.join()
            worker2.close()
            client.close()
            print("Server respond with nothing")
    except Exception as ex:
        print(ex)
        server.join()
        client.close()
        worker2.close()

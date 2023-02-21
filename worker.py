import socket 
import threading
import json
import subprocess


HEADER = 64
PORT = 5051
PORT2 = 0
SERVER = "127.0.0.1"
ADDR = (SERVER, PORT)
ADDR2 = (SERVER,PORT2)
FORMAT = 'utf-8'
DISCONNECT_MESSAGE = "!DISCONNECT"

JOBPROCESSOR = 4

worker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
worker.connect(ADDR)
worker2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
worker2.bind(ADDR2)

def send(msg):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    worker.send(send_length)
    worker.send(message)
    return (worker.recv(2048).decode(FORMAT))


def handle_server(conn, addr):
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

                if msg.startswith('simple:'):
                    msg = msg.replace('simple: ','')
                    msg = json.loads(msg)
                    failed = 0
                    returnCode = 1
                    try:
                        resultRunCommand = subprocess.check_output(msg['cmd'].split(' '))
                        returnCode = resultRunCommand.decode(FORMAT)
                    except:
                        failed = 1

                    sendResultOfExecutedCommand(conn, failed, returnCode)

                elif msg.startswith('executable'):
                    msg = msg.replace('executable: ','')
                    y = json.loads(msg)
                    print(y)
                    failed = 0
                    returnCode = 1
                    try:
                        # just assumed ['ptyhon',file_name]
                        bashCommand = json.loads(y['cmd'])['cmd']+" "+y['name']
                        process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
                        returnCode, error = process.communicate()

                    except:
                        failed = 1

                    sendResultOfExecutedCommand(conn, failed, returnCode.decode(FORMAT))
    except Exception as ex:
        print("connection closed unexpectedly")
        print(ex)
    conn.close()

def sendResultOfExecutedCommand(conn, failed, returnCode):
    if not failed:
        x = {
                    "result": returnCode
                    }
        y = json.dumps(x)

        conn.send("success ".encode(FORMAT)+str(y).encode(FORMAT))
    else:
        conn.send("failed ".encode(FORMAT))
        
def is_port_in_use(port: int) -> bool:
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('127.0.0.1', port)) == 0

def start():
    worker2.listen()
    print(f"[LISTENING] Worker is listening on {SERVER}")
    while True:
        conn, addr = worker2.accept()
        thread = threading.Thread(target=handle_server, args=(conn, addr))
        thread.start()
        print(f"[ACTIVE CONNECTIONS] {threading.activeCount() - 1}")



if __name__ == "__main__":
    print("[STARTING] worker is starting...")
    server = threading.Thread(target=start)
    server.start()

    config =  'workerStart: {"server":'+json.dumps(worker.getsockname()[0])+', "port":'+str(worker2.getsockname()[1])+', "jobs":'+str(JOBPROCESSOR)+'}'
    masterResponse =  send(config)

    if masterResponse.__contains__('OK'):
        send(masterResponse.split()[0]+" workerConnect")
        send(DISCONNECT_MESSAGE)
        worker.close()
 
        print('New Bind \n')
        # print(ADDR)
        start()



print(is_port_in_use(5050))
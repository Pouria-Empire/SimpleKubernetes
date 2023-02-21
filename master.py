import socket
import subprocess 
import threading
import json
import random 
import time

HEADER = 64
PORT = 5051
SERVER = socket.gethostbyname(socket.gethostname())
ADDR = (SERVER, PORT)
FORMAT = 'utf-8'
DISCONNECT_MESSAGE = "!DISCONNECT"
DISCONNECT_MESSAGE_SERVER = "!DOWNSERVER"

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(ADDR)

listOfWorkersData = [] # server port free capacity
listOfWorkers = [] #maintain sockets

exit = 0
allLogFile = open("log.txt", "a")

jobStatus = {"JobID":[],"Node":[],"Status":[]}
nodeStatus = {"NodeId":[],"Status":[]}

JOBID = 1
NODE_ID_CONNECTED = 1

clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

clientTokenSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
clientUsername = 'admin'
clientPassword = 'admin'

def sendMessage(msg, workFor):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    workFor.send(send_length)
    workFor.send(message)
    
    return workFor.recv(2048).decode(FORMAT)

def setLog(message,ID):
    global JOBID
    jobStatus["JobID"].append(JOBID)
    jobStatus["Node"].append("n"+str(ID))
    jobStatus["Status"].append(message)
    JOBID += 1

def setNodeStatus(message,ID):
    nodeStatus["NodeId"].append(ID)
    nodeStatus["Status"].append(message)


def handle_client(conn, addr):
    global NODE_ID_CONNECTED,exit,clientSocket,clientTokenSocket,clientUsername,clientPassword
    print(f"[NEW CONNECTION] {addr} connected.")
    ourNode=-1
    connected = True
    try:
        while connected:
            msg_length = conn.recv(HEADER).decode(FORMAT)
            if msg_length:
                msg_length = int(msg_length)
                msg = conn.recv(msg_length).decode(FORMAT)
                if msg == DISCONNECT_MESSAGE:
                    connected = False
                
                if msg==DISCONNECT_MESSAGE_SERVER:
                    exit=1
                    break

                print(f"[{addr}] {msg}")

                if msg.startswith("This is Client: "):
                    msg = msg.replace("This is Client: ",'')
                    y = json.loads(msg)
                    print(y)
                    add = (y['server'],y['port'])
                    if y['username'] == clientUsername and y['password'] == clientPassword:
                        try:
                            clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            clientSocket.connect(add)
                            conn.send("OK".encode(FORMAT))
                            clientTokenSocket = conn
                        except:
                            clientTokenSocket.close()
                            print('client not connected')
                    else:
                        print("Unautorized")
                        conn.send("Unautorized".encode(FORMAT))


                if msg.startswith('simple:'):                
                    SendRequestToWorkers(msg)
                    # conn.send("done".encode(FORMAT))
                    
                # just accept worker
                if msg.startswith('workerStart'):
                    msg = msg.replace('workerStart: ','')
                    y = json.loads(msg)
                    print(y)
                    listOfWorkersData.append([y['server'],y['port'],y['jobs'],y['jobs']])
                    conn.send(str(NODE_ID_CONNECTED).encode(FORMAT)+" OK".encode(FORMAT))
                    NODE_ID_CONNECTED += 1  # critical section

                # initializing connected worker
                if msg.endswith('workerConnect'):
                    y = listOfWorkersData[int(msg.split()[0])-1]
                    ourNode = int(msg.split()[0])
                    workFor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    add = (y[0],y[1])
                    try:
                        workFor.connect(add) 
                        listOfWorkers.append(workFor)
                        setNodeStatus("UP",int(msg.split()[0]))
                    except Exception as inst:
                        print(inst)
                        print("socket cannot initialize")
                        setNodeStatus("DOWN",int(msg.split()[0]))

                if msg.startswith('executable'):
                    msg = msg.replace('executable: ','')
                    y = json.loads(msg)
                    print(y)
                    file_name = "./tmp/"+str(random.randint(1,100))+y['name']
                    file_size = y['size']
                    
                    file = open(file_name, 'x')
                    #send ready message
                    conn.send("ready".encode(FORMAT))
                    # Running the loop while file is recieved.
                    k = int(file_size)
                    while 0 < k:
                        data = conn.recv(1024).decode('utf-8')
                        if not (data):
                            break
                        file.write(data)
                        k -= len(data)
                    file.close()
                    #send request to workers
                    x = {
                    "cmd": y['cmd'],
                    "name": file_name,
                    "size": file_size
                    }
                    y = json.dumps(x)
                    SendRequestToWorkers('executable: '+str(y))

                if msg == "wlf job list":
                    y = json.dumps(jobStatus)
                    conn.send(str(y).encode(FORMAT))

                if msg == "wlf node list":
                    y = json.dumps(nodeStatus)
                    conn.send(str(y).encode(FORMAT))
                
                if msg.startswith('wlf job logs'):
                    msg = msg.replace("wlf job logs ",'').split()
                    job_id = int(msg[0])
                    countLines = 5
                    
                    if len(msg)>1 and msg[1]=='-count':
                        countLines = int(msg[3])

                    countLines = min(countLines,len(jobStatus["JobID"]))
                    x = {"JobID":[],"Node":[],"Status":[]}

                    if msg.__contains__('-tail') or msg.__contains__('-head'):
                        if msg[1]=='-tail':
                            # x = [jobStatus[:][-i-1] for i in range(countLines) if jobStatus["JobID"][-i-1]==job_id]
                            x["Status"] = [jobStatus['Status'][-i-1] for i in range(countLines) if jobStatus["JobID"][-i-1]==job_id]
                            x["Node"] = [jobStatus["Node"][-i-1] for i in range(countLines) if jobStatus["JobID"][-i-1]==job_id]
                            x["JobID"] = [jobStatus["JobID"][-i-1] for i in range(countLines) if jobStatus["JobID"][-i-1]==job_id]
                        else:
                            x["Status"] = [jobStatus['Status'][i] for i in range(countLines) if jobStatus["JobID"][i]==job_id]
                            x["Node"] = [jobStatus["Node"][i] for i in range(countLines) if jobStatus["JobID"][i]==job_id]
                            x["JobID"] = [jobStatus["JobID"][i] for i in range(countLines) if jobStatus["JobID"][i]==job_id]
                    else:
                        x["Status"] = [jobStatus['Status'][i] for i in range(countLines) if jobStatus["JobID"][i]==job_id]
                        x["Node"] = [jobStatus["Node"][i] for i in range(countLines) if jobStatus["JobID"][i]==job_id]
                        x["JobID"] = [jobStatus["JobID"][i] for i in range(countLines) if jobStatus["JobID"][i]==job_id]

                    conn.send(str(json.dumps(x)).encode(FORMAT))

                if msg == 'wlf node top':
                    x = [str((int(x[3])-int(x[2]))/int(x[3]))+"%"+" of resources in used" for x in listOfWorkersData] # return used in percent
                    conn.send(str(json.dumps(x)).encode(FORMAT))

                
                print("Msg received".encode(FORMAT))
    except Exception as ex:
        print(ex)
        setNodeStatus("DOWN",ourNode)

    conn.close()


def SendRequestToWorkers(msg):
    global clientTokenSocket
    clientTokenSocket.send("done".encode(FORMAT))
    logClient(msg, 'Searching for Worker')

    for ID,workFor in enumerate(listOfWorkers):
        setLog("Pending",ID)
        if listOfWorkersData[ID][2] >0 :
            listOfWorkersData[ID][2] -= 1   # remember after complete +1 requiered

            setLog("Running",ID)
   
            logClient(msg, 'Found Worker and Job passed to it')

            thread = threading.Thread(target=waitForWorker, args=(msg, ID, workFor))
            thread.start()

            break

def waitForWorker(msg, ID, workFor):
    response = sendMessage(msg, workFor)

    logClient(response, 'Job done and response catched')

    if response.__contains__('success'):
        setLog("success",ID)
    else:
        setLog("failed",ID) 
                        
    listOfWorkersData[ID][2] += 1
                        
    allLogFile.write(response+"\n")
    allLogFile.flush()

def logClient(msg, strf):
    x = {
    "time": time.ctime(),
    "command": msg,
    "status":strf
    }
    y = json.dumps(x)
    sendMessage('log: '+str(y),clientSocket)


def start():
    server.listen()
    print(f"[LISTENING] Server is listening on {SERVER}")
    while exit==0:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()
        print(f"[ACTIVE CONNECTIONS] {threading.activeCount() - 1}")
    
    print("server going to down")
    

if __name__ == "__main__":
    print("[STARTING] server is starting...")
    try:
        start()
    except Exception as ex:
        print(ex)
    
    for worker in listOfWorkers:
        worker.close()
    allLogFile.close()
    server.close()

    # re run master
    # bashCommand = 'python master.py'
    # process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
    # returnCode, error = process.communicate()

import atexit
import os
import socket
import sys
import subprocess
import psutil
import shutil
from pathlib import Path, PurePath
import time
from db import WorkerDB
from rc import bash, run
from multiprocessing import Process
import requests

os.environ["ADVERSARY_CONSENT"] = "1"

port = 3035
sckt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sckt.bind(('0.0.0.0', port))
sckt.listen()


def enough_space(filename="/datadrive"):
    try:
        df = subprocess.Popen(["df", filename], stdout=subprocess.PIPE, universal_newlines=True)
        output = df.communicate()[0]
        pr = output.split()[11]
        n_pr = int(str(pr)[:-1])
        if n_pr >= 65:
            return False
        return True
    except:
        return False


def install_new_packages(thread_n):
    try:
        print("Install new packages")
        f = open(f'''{thread_n}/pytest/requirements.txt''', 'r')
        required = {l.strip().lower() for l in f.readlines()}
        p = bash(f'''pip3 freeze''')
        rr = p.stdout.split('\n')
        installed = {k.split('==')[0].lower() for k in rr if k}
        missing = required - installed
        print(missing)
        if missing:
            python = sys.executable
            subprocess.check_call([python, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL)
    except Exception as e:
        print(e)

def build(sha):
    if not enough_space():
        print("Not enough space.")
        bld = bash(f'''rm -rf neacore''')
    with open('build_out', 'w') as fl_o:
        with open('build_err', 'w') as fl_e:
            kwargs = {"stdout": fl_o, "stderr": fl_e}
            print("Checkout")
            bld = bash(f'''
                cd nearcore
                git fetch
                git checkout {sha}
            ''' , **kwargs, login=True)
            print(bld)
            if bld.returncode != 0:
                print("Clone")
                bld = bash(f'''
                    rm -rf nearcore
                    git clone https://github.com/nearprotocol/nearcore
                    cd nearcore
                    git checkout {sha}
                ''' , **kwargs, login=True)
                print(bld)
                if bld.returncode != 0:
                    return bld.stderr
            print("Build")
            bld = bash(f'''
                cd nearcore
                cargo build -j2 -p neard --features adversarial
              ''' , **kwargs, login=True)
            print(bld)
            if bld.returncode != 0:
                bld = bash(f'''rm -rf nearcore''')
                return bld.stderr
            return ''    


def keep_pulling(ip_address):
    server = WorkerDB()
    status, request_id = server.get_instance_status(ip_address)
    if status != 'AVAILABLE':
        request_status = server.get_request_data(request_id)['status']
        if request_status == 'CANCELED':
            server.update_instance_status(ip_address, 'AVAILABLE')
    while True:
        server = WorkerDB()
        status, request_id = server.get_instance_status(ip_address)
        if status == 'TAKEN':
            request_data = server.get_request_data(request_id)
            server.update_instance_status(ip_address, 'BUILDING')
            err = build(request_data['sha'])
            server = WorkerDB()
            status_updated, request_id_updated = server.get_instance_status(ip_address)
            if status_updated == 'BUILDING' and request_id_updated == request_id:
                if err == '':
                    server.update_instance_status(ip_address, 'READY')
                else:
                    server.update_instance_status(ip_address, 'BUILD FAILED')
            else:
                server.update_instance_status(ip_address, 'AVAILABLE')
        time.sleep(5)

def cleanup(ip_address):
    sckt.close()
    bash(f'''killall -9 neard
    killall -2 cargo
    killall -9 cargo
    ''')
    server = WorkerDB()
    status_updated, _ = server.get_instance_status(ip_address)
    if status_updated == 'BUILDING':
         server.update_instance_status(ip_address, 'BUILD FAILED')
            

if __name__ == "__main__":
    ip_address = requests.get('https://checkip.amazonaws.com').text.strip()
    print(ip_address)
    atexit.register(cleanup, ip_address)
    keep_pulling(ip_address)
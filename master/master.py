from flask import Flask, session, flash, render_template, redirect, json, url_for, request, abort, make_response, jsonify, send_file
from rc import bash, ok
import requests
import os
import json

from db import MasterDB
from db import NayDuckDB

app = Flask(__name__)

app.config['TEMPLATES_AUTO_RELOAD'] = True

def is_allowed(token):
    allowed = False
    server = NayDuckDB()
    github_login = server.get_github_login(token)
    if not github_login:
        return {'allowed': allowed, 'response': 'Failure. NayDuck token is not found. Log in with NayDuck first.'}
    allowed = True
    return {'allowed': allowed}

def run_remote_cmd(ip, cmd):
    print(cmd)
    res = bash(f'''
        ssh azureuser@{ip} \'export PATH=$HOME/.cargo/bin:$PATH; {cmd}  &>>~/.testnode_log &\' 
    ''')
    print(res)
    return {'stderr': res.stderr}


def upload_to_remote(ip, fl_name, cnt):
    with open(fl_name, "w") as fl:
        fl.write(cnt) 
    res = bash(f'''
        scp {fl_name} azureuser@{ip}:~/.near
    ''')
    print(res)
    return {'stderr': res.stderr}

@app.route('/request_a_run', methods=['POST', 'GET'])
def request_a_run():
    request_json = request.get_json(force=True)
    permission = is_allowed(request_json['token'])
    if not permission['allowed']:
        return jsonify({'code': -1, 'err': permission['response']})
    server = MasterDB()
    if not request_json['sha']:
        resp = {'code': 1, 'response': 'Failure. Git sha were not provided.'}
        return jsonify(resp)
    fetch = bash(f'''
            rm -rf nearcore
            git clone https://github.com/nearprotocol/nearcore
            cd nearcore
            git fetch 
            git checkout {request_json['sha']}
    ''')
    if fetch.returncode == 0:
        title = bash(f'''
            cd nearcore
            git log --format='%s' {request_json['sha']}^!
        ''').stdout
        request_id = server.scheduling_a_run(
                                  sha=request_json['sha'],
                                  num_nodes=request_json['num_nodes'],
                                  title=title,
                                  requester=request_json['requester'])

        resp = {'code': 0, 'request_id': request_id}
    else:
        resp = {'code': -1, 'err': fetch.stderr}
    return jsonify(resp)

@app.route('/get_instances', methods=['POST', 'GET'])
def get_instances():
    request_json = request.get_json(force=True)
    permission = is_allowed(request_json['token'])
    if not permission['allowed']:
        return jsonify({'code': -1, 'err': permission['response']})
    
    server = MasterDB()
    instances = server.get_instances(request_json['num_nodes'], request_json['request_id'])
    print(instances)
    return jsonify({'ips': instances})

@app.route('/get_instances_status', methods=['POST', 'GET'])
def get_instances_status():
    request_json = request.get_json(force=True)
    permission = is_allowed(request_json['token'])
    if not permission['allowed']:
        return jsonify({'code': -1, 'err': permission['response']})
    
    server = MasterDB()
    return jsonify(server.get_instances_status(request_json['ips']))

@app.route('/cancel_the_run', methods=['POST', 'GET'])
def cancel_the_run():
    request_json = request.get_json(force=True)
    permission = is_allowed(request_json['token'])
    if not permission['allowed']:
        return jsonify({'code': -1, 'err': permission['response']})
    
    server = MasterDB()
    ips = server.cancel_the_run(request_json['request_id'])
    for ip in ips:
        run_remote_cmd(ip, "killall -9 neard")
        run_remote_cmd(ip, "killall -9 cargo")
    server.free_instances(request_json['request_id'])
    return jsonify({})

@app.route('/run_cmd', methods=['POST', 'GET'])
def run_cmd():
    request_json = request.get_json(force=True)
    permission = is_allowed(request_json['token'])
    if not permission['allowed']:
        return jsonify({'code': -1, 'err': permission['response']})
    resp = run_remote_cmd(request_json['ip'], request_json['cmd'])
    return jsonify(resp)


@app.route('/upload', methods=['POST', 'GET'])
def upload():
    request_json = request.get_json(force=True)
    permission = is_allowed(request_json['token'])
    if not permission['allowed']:
        return jsonify({'code': -1, 'err': permission['response']})
    resp = upload_to_remote(request_json['ip'], request_json['fl_name'], request_json['cnt'])
    return jsonify(resp)


@app.route('/cleanup', methods=['POST', 'GET'])
def cleanup():
    request_json = request.get_json(force=True)
    permission = is_allowed(request_json['token'])
    if not permission['allowed']:
        return jsonify({'code': -1, 'err': permission['response']})
    resp = run_remote_cmd(request_json['ip'], "rm -r ~/.testnode_log ~/.near/*")
    return jsonify(resp)


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
    
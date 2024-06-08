from flask import Flask, render_template, request
from client import GFSClient
import rpyc
import sys

app = Flask(__name__)
client = None

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/create', methods=['POST'])
def create_file():
    file_name = request.form['file_name']
    data = request.form['data']
    try:
        client.create(file_name, data)
        return "File created successfully!"
    except Exception as e:
        return str(e)

@app.route('/read', methods=['POST'])
def read_file():
    file_name = request.form['file_name']
    try:
        data = client.read(file_name)
        return f"Data read from {file_name}: {data}"
    except Exception as e:
        return str(e)

@app.route('/append', methods=['POST'])
def append_file():
    file_name = request.form['file_name']
    data = request.form['data']
    try:
        client.append(file_name, data)
        return "Data appended successfully!"
    except Exception as e:
        return str(e)

@app.route('/delete', methods=['POST'])
def delete_file():
    file_name = request.form['file_name']
    try:
        client.delete(file_name)
        return "File deleted successfully!"
    except Exception as e:
        return str(e)

@app.route('/list', methods=['GET'])
def list_files():
    try:
        files = client.list_files()
        return "Files in the GFS: " + ", ".join(files)
    except Exception as e:
        return str(e)

if __name__ == '__main__':
    try:
        con = rpyc.connect("localhost", port=4531)
        client = GFSClient(con.root.GFSMaster())
    except EnvironmentError:
        print("Cannot establish connection with GFSMaster")
        print("Connection Error: Please start master.py and try again")
        sys.exit(1)
    
    app.run(debug=True)

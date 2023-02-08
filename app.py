# -*- coding: UTF-8 -*-

from flask import Flask, render_template, request, jsonify, json
app = Flask(__name__)

@app.route('/')
def webapi():
    return render_template('home.html')

@app.route('/data/message', methods=['GET'])
def getDataMessage():
    if request.method == "GET":
        with open('data/message.json', 'r') as f:
            data = json.load(f)
            print("text : ", data)
        f.close
        return jsonify(data)  # 直接回傳 data 也可以，都是 json 格式

@app.route('/data/message', methods=['POST'])
def setDataMessage():
    if request.method == "POST":
        data = {
            'appInfo': {
                'id': request.form['app_id'],
                'name': request.form['app_name'],
            }
        }
        print(type(data))
        with open('data/message.json', 'w') as f:
            json.dump(data, f)
        f.close
        return jsonify(result='OK')

if __name__ == '__main__':
    app.run(host="0.0.0.0",debug=True)
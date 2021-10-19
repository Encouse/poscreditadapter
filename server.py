from flask import Flask
from flask import request, Response
from database import clients, orders
from bson.json_util import dumps

app = Flask(__name__)


@app.route("/clients", methods=['GET', 'POST'])
def clients_data():
    if request.method == 'POST':
        data = request.get_json(force=True)
        client = clients.insert_one(data)
        return Response(
            {'success': True},
            mimetype='application/json',
        )
    elif request.method == 'GET':
        return Response(
            dumps(
                {'results': [doc for doc in clients.find().sort('_id', 1).limit(50)]}),
            mimetype='application/json',
        )


@app.route("/orders", methods=['GET'])
def orders_data():
    return Response(
        dumps(
            {'results': [doc for doc in orders.find().sort('_id', 1).limit(50)]}),
        mimetype='application/json',
    )
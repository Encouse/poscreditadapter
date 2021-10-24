from flask import Flask
from flask import request, Response
from tasks import send_mail
from settings import *

from database import clients, orders, warnings
from bson.json_util import dumps

app = Flask(__name__)


@app.route("/orders", methods=['GET'])
def orders_data():
    return Response(
        dumps(
            {'results': list(orders.find().sort('_id', 1).limit(50))}
        ),
        mimetype='application/json',
    )


@app.route("/tinkoff_webhook", methods=["POST"])
def tinkoff_webhook():
    data = request.get_json()
    site_url = f"{SITE_URL}/#besprc"
    if data['status'] == 'rejected':
        send_mail(data['email'], 'Получен отказ на оформление рассрочки на курс',
                  TINKOFF_REJECT_MESSAGE.format(data['model'], data['price'],
                                                site_url, site_url))
    elif data['status'] == 'approved':
        send_mail(data['email'], 'Получена заявка на оформление рассрочки на курс',
                  ACCEPTED_WARNING_EMAIL_TEXT.format(data['model'], data['price']))
    elif data['status'] == 'canceled':
        send_mail(data['email'], 'Жаль, что вы отказались от рассрочки!',
                  SIGNING_REJECT_MESSAGE.format(site_url, site_url, site_url))
    return Response({'success': True})

from bs4 import BeautifulSoup
from parse import parse_order_details, parse_order_table
from request import *


session = get_poscredit_session()
response = get_poscredit_orders(session, status='Ошибочный ввод')

soup = BeautifulSoup(response.text, "html.parser")

data = parse_order_table(soup)


for item in data:
    response = get_order_details(session, item['id'])
    soup = BeautifulSoup(response.text, "html.parser")
    print(parse_order_details(soup))
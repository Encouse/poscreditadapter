

def parse_order_table(soup):
    HEADERS = ((0, 'id'), (1, 'name'), (2, 'date'), (3, 'status'), (4, 'signed'))

    data = []
    trs = soup.find_all('tr')
    headers_dict = dict(HEADERS)

    for tr in trs[1:]:
        tds = tr.find_all('td')
        obj = {}
        for idx, td in enumerate(tds):
            cts = td.contents
            if len(cts) > 1:
                obj[headers_dict[idx]] = cts[1].text
        data.append(obj)

    return data


def parse_order_details(soup):
    HEADERS = ((0, 'name'), (1, 'type'), (2, 'id'), (3, 'price'))
    headers_dict = dict(HEADERS)
    datablock = soup.find('div', class_='request-personal-data')

    phone = datablock.find_all()[3].text
    items = []

    items_table = soup.find('table', class_='tbl-1')
    items_rows = items_table.find_all('tr', class_='tbl-row')
    for row in items_rows:
        tds = row.find_all('td')
        item = {}
        for idx, col in enumerate(tds):
            cts = col.contents
            if len(cts) > 1:
                item[headers_dict[idx]] = cts[1].text
        items.append(item)

    return {'phone': phone, 'items': items}


def parse_bank_questionnaire_data(soup):
    email = soup.find(attrs={'name': 'EMail'}).attrs['value']
    model = soup.find(id='good_brand_id').attrs['value']
    return {'email': email, 'model': model}
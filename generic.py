

from bs4 import BeautifulSoup
from parse import parse_bank_questionnaire_data
from request import get_bank_questionnaire, get_poscredit_bank_session


def get_questionnaire_data(id, hash):
    session = get_poscredit_bank_session()
    questionnaire = get_bank_questionnaire(
        session, id, hash)
    soup = BeautifulSoup(questionnaire.text, "html.parser")
    data = parse_bank_questionnaire_data(soup)
    return data
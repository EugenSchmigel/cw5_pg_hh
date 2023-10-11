from config import config
from utils import insert_employers_to_db, get_data_from_hh, insert_vacancies_to_db, create_db_tables
from DBManager import DBManager


# selected employers id from HH
employers_ids = [
    1740,  # "Яндекс"
    2968758,   # "Смарт Конекшн"
    9897931,   # "ЕКОМ-Сервис"
]

database_name = 'hh'
# read database connection from .ini file
params = config()
# create database tables
create_db_tables(database_name, params)

# insert employers to database
insert_employers_to_db(get_data_from_hh(employers_ids), database_name, **params)
# insert employers to database
insert_vacancies_to_db(get_data_from_hh(employers_ids), database_name, **params)

# class DBManager
db = DBManager(database_name, params)

print(db.get_companies_and_vacancies_count())
print(db.get_all_vacancies())
print(db.get_avg_salary())
print(db.get_vacancies_with_highest_salary())
print(db.get_vacancies_with_keyword('менеджер'))

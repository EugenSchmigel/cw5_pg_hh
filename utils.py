import psycopg2
import requests


def get_data_from_hh(employers_ids):
    """ read data from HH """
    hh_data = []
    for id in employers_ids:
        url = f'https://api.hh.ru/employers/{id}'
        employers_response = requests.get(url)
        data_employer = employers_response.json()
        vacancy_response = requests.get(data_employer["vacancies_url"])
        vacancy_data = vacancy_response.json()
        hh_data.append({
            'company': data_employer,
            'vacancies': vacancy_data['items']
        })

    return hh_data




def insert_employers_to_db(data, database_name, **params):
    """ # insert employers to database """
    conn = psycopg2.connect(database=database_name, **params)
    for employer in data:
        with conn.cursor() as cur:
            cur.execute('INSERT INTO companies (company_id, company_name, description)'
                           'VALUES (%s, %s, %s)'
                            'ON CONFLICT (company_id) DO NOTHING',
                           (employer["company"].get("id"), employer["company"].get("name"),
                            employer["company"].get("description")
                            ))

    conn.commit()
    conn.close()


def create_db_tables(database_name, params):
    """  create database tables 'employers & vacancies' """
    conn = psycopg2.connect(database=database_name, **params)
    with conn.cursor() as cur:
        cur.execute("""CREATE TABLE IF NOT EXISTS companies(
                       company_id int PRIMARY KEY,
                       company_name VARCHAR,
                       description VARCHAR)""")

        cur.execute("""CREATE TABLE IF NOT EXISTS vacancies(
                               vacancy_id int PRIMARY KEY,
                               company_id int REFERENCES companies(company_id),
                               vacancy_name VARCHAR,
                               salary INTEGER,
                               description VARCHAR,
                               experience VARCHAR
                               )""")
    conn.commit()
    conn.close()


def salary_filter(salary):
    if salary is not None:
        if salary['from'] is not None and salary['to'] is not None:
            return salary['from'] + salary['to']
        elif salary['from'] is not None:
            return salary['from']
        elif salary['to'] is not None:
            return salary['to']
    return 0


def insert_vacancies_to_db(data, database_name, **params):
    """ # insert vacancies to database """
    conn = psycopg2.connect(database=database_name, **params)
    for company in data:
        with conn.cursor() as cur:
            for vacancy in company['vacancies']:
                salary = salary_filter(vacancy["salary"])
                cur.execute('INSERT INTO vacancies'
                               '(vacancy_id, company_id, vacancy_name, salary, description, experience)'
                               'VALUES (%s, %s, %s, %s, %s, %s)'
                                'ON CONFLICT (vacancy_id) DO NOTHING',
                               (str(vacancy["id"]), str(company["company"].get("id")), str(vacancy["name"]), str(salary),
                                str(vacancy["snippet"].get("responsibility")),
                                str(vacancy["experience"].get("name"))))

    conn.commit()
    conn.close()
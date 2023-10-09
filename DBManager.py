import psycopg2
from config import config


class DBManager:
    def __init__(self, database_name, params=config()):
        self.params = params
        self.database_name = database_name


    def get_companies_and_vacancies_count(self):
        """ получает список всех компаний и количество вакансий у каждой компании. """
        conn = psycopg2.connect(database=self.database_name, **self.params)
        with conn.cursor() as cur:
            cur.execute('SELECT COUNT(vacancy_id)'
                           'FROM companies'
                           'JOIN vacancies USING(company_id);')

            data = cur.fetchall()

        conn.close()
        return data

    def get_all_vacancies(self):
        """ получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию. """
        conn = psycopg2.connect(database=self.database_name, **self.params)
        with conn.cursor() as cur:
            cur.execute('SELECT vacancy_name, company_name'
                           'FROM vacancies'
                           'JOIN companies USING (company_id);')

            data = cur.fetchall()

        conn.close()
        return data

    def get_avg_salary(self):
        """ получает среднюю зарплату по вакансиям """
        conn = psycopg2.connect(database=self.database_name, **self.params)
        with conn.cursor() as cur:
            cur.execute('SELECT round(AVG(salary)) AS average_salary '
                           'FROM companies '
                           'JOIN vacancies USING (company_id);')

            data = cur.fetchall()

        conn.close()
        return data

    def get_vacancies_with_highest_salary(self):
        """ получает список всех вакансий, у которых зарплата выше средней по всем вакансиям. """
        conn = psycopg2.connect(database=self.database_name, **self.params)
        with conn.cursor() as cur:
            cur.execute('SELECT * FROM vacancies WHERE salary in (SELECT AVG(salary) FROM vacancies);')

            data = cur.fetchall()

        conn.close()
        return data

    def get_vacancies_with_keyword(self, keyword):
        """ получает список всех вакансий, в названии которых содержатся переданные в метод слова, например python. """
        conn = psycopg2.connect(database=self.database_name, **self.params)
        with conn.cursor() as cur:
            cur.execute(f"""
            SELECT * 
                FROM vacancies
                WHERE (vacancy_name) LIKE '%{keyword}%'
                OR (vacancy_name) LIKE '%{keyword}%'""")

            data = cur.fetchall()

        conn.close()
        return data
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
            cur.execute("""
                SELECT COUNT(*) AS vacancy_count, company_id, company_name
                FROM vacancies 
                JOIN companies USING(company_id)
                GROUP BY company_id, company_name;
                """)

            data = cur.fetchall()

        conn.close()
        return data

    def get_all_vacancies(self):
        """ получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию. """
        conn = psycopg2.connect(database=self.database_name, **self.params)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT company_name, vacancy_name, salary
                FROM vacancies
                JOIN companies USING(company_id);
                """)
            data = cur.fetchall()

        conn.close()
        return data

    def get_avg_salary(self):
        """ получает среднюю зарплату по вакансиям """
        conn = psycopg2.connect(database=self.database_name, **self.params)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT AVG(salary) FROM vacancies;
                """)

            data = cur.fetchall()

        conn.close()
        return data

    def get_vacancies_with_highest_salary(self):
        """ получает список всех вакансий, у которых зарплата выше средней по всем вакансиям. """
        conn = psycopg2.connect(database=self.database_name, **self.params)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT vacancy_id, vacancy_name, salary
                FROM vacancies
                WHERE salary > (
                  SELECT AVG(salary) AS avg_salary
                  FROM vacancies
                );
                """)


            data = cur.fetchall()

        conn.close()
        return data

    def get_vacancies_with_keyword(self, keyword):
        """ получает список всех вакансий, в названии которых содержатся переданные в метод слова, например python. """
        conn = psycopg2.connect(database=self.database_name, **self.params)
        with conn.cursor() as cur:
            cur.execute(f"""
                        SELECT * FROM vacancies WHERE LOWER(vacancy_name) LIKE LOWER('%{keyword}%')
                        """)

            data = cur.fetchall()

        conn.close()
        return data
import unittest

import mysql.connector
import random, datetime

import os
from dotenv import load_dotenv
load_dotenv()


def gen_insert_query(rows_num):
    condidate_types = []
    condidate_types.append('("Nikolaeva Violetta", "2005-06-27", "woman", "Information Security of the Higher School of Economics"),')
    condidate_types.append('("Nikolaev Georgy", "2005-01-21", "man", "System analysis of MEPhI"),')
    condidate_types.append('("Tkacheva Vera", "2005-10-04", "woman", "HSE Business Informatics"),')
    condidate_types.append('("Ovodov Dmitry", "2005-07-15", "man", "Information Security of the Higher School of Economics"),')
    condidate_types.append('("Savkina Polina", "2005-06-15", "woman", "Moscow State University Faculty of Law"),')
    condidate_types.append('("Zmaeva Sveta", "2005-04-22", "woman", "Faculty of Design of Moscow State University"),')
    condidate_types.append('("Zaitsev Fedor", "2005-12-16", "man", "Computer Science and Control Systems of Bauman Moscow State Technical University"),')
    condidate_types.append('("Ivanov Maxim", "1979-08-12", "man", "Oxford Faculty of Business"),')
    condidate_types.append('("Sophronova Evgenia", "2003-02-22", "woman", "Faculty of Graphics of the Stroganov Moscow State Academy of Art and Industry"),')
    condidates = ","
    for i in range(rows_num):
        condidates += condidate_types[random.randint(0, len(condidate_types) - 1)]
    return condidates[:(-1)]

connection = mysql.connector.connect(host = os.getenv('DB_HOST'), 
                                     user = os.getenv('DB_USER'), 
                                     passwd = os.getenv('DB_PASSWORD')
                                     )
cursor=connection.cursor()
cursor.execute(f'CREATE DATABASE IF NOT EXISTS {os.getenv('DB_DATABASE')}')

cursor.execute(f"""CREATE TABLE IF NOT EXISTS {os.getenv('DB_DATABASE')}.internship_candidates(
    name VARCHAR(100),
    birth_date DATE,
    sex VARCHAR(5),
    education VARCHAR(150))
""")
default_data = """("Nikolaeva Violetta", "2005-06-27", "woman", "Information Security of the Higher School of Economics"),
                ("Nikolaev Georgy", "2005-01-21", "man", "System analysis of MEPhI"),
                ("Tkacheva Vera", "2005-10-04", "woman", "HSE Business Informatics"),
                ("Ovodov Dmitry", "2005-07-15", "man", "Information Security of the Higher School of Economics"),
                ("Savkina Polina", "2005-06-15", "woman", "Moscow State University Faculty of Law"),
                ("Zmaeva Svetlana", "2005-04-22", "woman", "Faculty of Design of Moscow State University"),
                ("Zaitsev Fedor", "2005-12-16", "man", "Computer Science and Control Systems of Bauman Moscow State Technical University"),
                ("Ivanov Maxim", "1979-08-12", "man", "Oxford Faculty of Business"),
                ("Sophronova Evgenia", "2003-02-22", "woman", "Faculty of Graphics of the Stroganov Moscow State Academy of Art and Industry")"""
cursor.execute(f'INSERT IGNORE INTO {os.getenv('DB_DATABASE')}.internship_candidates (name, birth_date, sex, education) VALUES {default_data}{gen_insert_query(10000)}')
connection.commit()



class SelectFuncTest(unittest.TestCase):
    
    def test_name_index_Nikolaeva_Violetta(self):
        
        cursor.execute(f'SELECT birth_date, sex, education FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE name LIKE "Nikolaeva Violetta"')
        result_without_index=cursor.fetchall()
        
        cursor.execute(f'CREATE INDEX name_index ON {os.getenv('DB_DATABASE')}.internship_candidates (name)')
        
        cursor.execute(f'SELECT birth_date, sex, education FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE name LIKE "Nikolaeva Violetta"')
        result_with_index=cursor.fetchall()
        
        cursor.execute(f'DROP INDEX name_index ON {os.getenv('DB_DATABASE')}.internship_candidates')
        
        self.assertEqual(result_without_index, result_with_index)
        
    def test_name_index_Nikolaev_Georgy(self):
        
        cursor.execute(f'SELECT birth_date, sex, education FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE name LIKE "Nikolaev Georgy"')
        result_without_index=cursor.fetchall()
        
        cursor.execute(f'CREATE INDEX name_index ON {os.getenv('DB_DATABASE')}.internship_candidates (name)')
        
        cursor.execute(f'SELECT birth_date, sex, education FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE name LIKE "Nikolaev Georgy"')
        result_with_index=cursor.fetchall()
        
        cursor.execute(f'DROP INDEX name_index ON {os.getenv('DB_DATABASE')}.internship_candidates')
        
        self.assertEqual(result_without_index, result_with_index)
        
    def test_name_index_Tkacheva_Vera(self):
        
        cursor.execute(f'SELECT birth_date, sex, education FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE name LIKE "Tkacheva Vera"')
        result_without_index=cursor.fetchall()
        
        cursor.execute(f'CREATE INDEX name_index ON {os.getenv('DB_DATABASE')}.internship_candidates (name)')
        
        cursor.execute(f'SELECT birth_date, sex, education FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE name LIKE "Tkacheva Vera"')
        result_with_index=cursor.fetchall()
        
        cursor.execute(f'DROP INDEX name_index ON {os.getenv('DB_DATABASE')}.internship_candidates')
        
        self.assertEqual(result_without_index, result_with_index)
        
    def test_name_index_Ovodov_Dmitry(self):
        
        cursor.execute(f'SELECT birth_date, sex, education FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE name LIKE "Ovodov Dmitry"')
        result_without_index=cursor.fetchall()
        
        cursor.execute(f'CREATE INDEX name_index ON {os.getenv('DB_DATABASE')}.internship_candidates (name)')
        
        cursor.execute(f'SELECT birth_date, sex, education FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE name LIKE "Ovodov Dmitry"')
        result_with_index=cursor.fetchall()
        
        cursor.execute(f'DROP INDEX name_index ON {os.getenv('DB_DATABASE')}.internship_candidates')
        
        self.assertEqual(result_without_index, result_with_index)
    
    def test_name_index_Savkina_Polina(self):
        
        cursor.execute(f'SELECT birth_date, sex, education FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE name LIKE "Savkina Polina"')
        result_without_index=cursor.fetchall()
        
        cursor.execute(f'CREATE INDEX name_index ON {os.getenv('DB_DATABASE')}.internship_candidates (name)')
        
        cursor.execute(f'SELECT birth_date, sex, education FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE name LIKE "Savkina Polina"')
        result_with_index=cursor.fetchall()
        
        cursor.execute(f'DROP INDEX name_index ON {os.getenv('DB_DATABASE')}.internship_candidates')
        
        self.assertEqual(result_without_index, result_with_index)
        
    def test_name_index_Zmaeva_Svetlana(self):
        
        cursor.execute(f'SELECT birth_date, sex, education FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE name LIKE "Zmaeva Svetlana"')
        result_without_index=cursor.fetchall()
        
        cursor.execute(f'CREATE INDEX name_index ON {os.getenv('DB_DATABASE')}.internship_candidates (name)')
        
        cursor.execute(f'SELECT birth_date, sex, education FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE name LIKE "Zmaeva Svetlana"')
        result_with_index=cursor.fetchall()
        
        cursor.execute(f'DROP INDEX name_index ON {os.getenv('DB_DATABASE')}.internship_candidates')
        
        self.assertEqual(result_without_index, result_with_index)
        
    def test_name_index_Zaitsev_Fedor(self):
        
        cursor.execute(f'SELECT birth_date, sex, education FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE name LIKE "Zaitsev Fedor"')
        result_without_index=cursor.fetchall()
        
        cursor.execute(f'CREATE INDEX name_index ON {os.getenv('DB_DATABASE')}.internship_candidates (name)')
        
        cursor.execute(f'SELECT birth_date, sex, education FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE name LIKE "Zaitsev Fedor"')
        result_with_index=cursor.fetchall()
        
        cursor.execute(f'DROP INDEX name_index ON {os.getenv('DB_DATABASE')}.internship_candidates')
        
        self.assertEqual(result_without_index, result_with_index)
        
    def test_name_index_Ivanov_Maxim(self):
        
        cursor.execute(f'SELECT birth_date, sex, education FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE name LIKE "Ivanov Maxim"')
        result_without_index=cursor.fetchall()
        
        cursor.execute(f'CREATE INDEX name_index ON {os.getenv('DB_DATABASE')}.internship_candidates (name)')
        
        cursor.execute(f'SELECT birth_date, sex, education FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE name LIKE "Ivanov Maxim"')
        result_with_index=cursor.fetchall()
        
        cursor.execute(f'DROP INDEX name_index ON {os.getenv('DB_DATABASE')}.internship_candidates')
        
        self.assertEqual(result_without_index, result_with_index)
    
    def test_name_index_Sophronova_Evgenia(self):
        
        cursor.execute(f'SELECT birth_date, sex, education FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE name LIKE "Sophronova Evgenia"')
        result_without_index=cursor.fetchall()
        
        cursor.execute(f'CREATE INDEX name_index ON {os.getenv('DB_DATABASE')}.internship_candidates (name)')
        
        cursor.execute(f'SELECT birth_date, sex, education FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE name LIKE "Sophronova Evgenia"')
        result_with_index=cursor.fetchall()
        
        cursor.execute(f'DROP INDEX name_index ON {os.getenv('DB_DATABASE')}.internship_candidates')
        
        self.assertEqual(result_without_index, result_with_index)
    
    def test_education_index_Information_Security_of_the_Higher_School_of_Economics(self):
        
        cursor.execute(f'SELECT name, birth_date, sex FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE education LIKE "Information Security%"')
        result_without_index=cursor.fetchall()
        
        cursor.execute(f'CREATE INDEX education_index ON {os.getenv('DB_DATABASE')}.internship_candidates (education)')
        
        cursor.execute(f'SELECT name, birth_date, sex FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE education LIKE "Information Security%"')
        result_with_index=cursor.fetchall()
        
        cursor.execute(f'DROP INDEX education_index ON {os.getenv('DB_DATABASE')}.internship_candidates')
        
        self.assertEqual(result_without_index, result_with_index)
    
    def test_education_index_System_analysis_of_MEPhI(self):
        
        cursor.execute(f'SELECT name, birth_date, sex FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE education LIKE "System analysis of MEPhI"')
        result_without_index=cursor.fetchall()
        
        cursor.execute(f'CREATE INDEX education_index ON {os.getenv('DB_DATABASE')}.internship_candidates (education)')
        
        cursor.execute(f'SELECT name, birth_date, sex FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE education LIKE "System analysis of MEPhI"')
        result_with_index=cursor.fetchall()
        
        cursor.execute(f'DROP INDEX education_index ON {os.getenv('DB_DATABASE')}.internship_candidates')
        
        self.assertEqual(result_without_index, result_with_index)
        
    def test_education_index_HSE_Business_Informatics(self):
        
        cursor.execute(f'SELECT name, birth_date, sex FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE education LIKE "HSE Business Informatics"')
        result_without_index=cursor.fetchall()
        
        cursor.execute(f'CREATE INDEX education_index ON {os.getenv('DB_DATABASE')}.internship_candidates (education)')
        
        cursor.execute(f'SELECT name, birth_date, sex FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE education LIKE "HSE Business Informatics"')
        result_with_index=cursor.fetchall()
        
        cursor.execute(f'DROP INDEX education_index ON {os.getenv('DB_DATABASE')}.internship_candidates')
        
        self.assertEqual(result_without_index, result_with_index)
        
    def test_education_index_Moscow_State_University_Faculty_of_Law(self):
        
        cursor.execute(f'SELECT name, birth_date, sex FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE education LIKE "Moscow State University Faculty of Law"')
        result_without_index=cursor.fetchall()
        
        cursor.execute(f'CREATE INDEX education_index ON {os.getenv('DB_DATABASE')}.internship_candidates (education)')
        
        cursor.execute(f'SELECT name, birth_date, sex FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE education LIKE "Moscow State University Faculty of Law"')
        result_with_index=cursor.fetchall()
        
        cursor.execute(f'DROP INDEX education_index ON {os.getenv('DB_DATABASE')}.internship_candidates')
        
        self.assertEqual(result_without_index, result_with_index)
        
    def test_education_index_Faculty_of_Design_of_Moscow_State_University(self):
        
        cursor.execute(f'SELECT name, birth_date, sex FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE education LIKE "Faculty of Design of Moscow State University"')
        result_without_index=cursor.fetchall()
        
        cursor.execute(f'CREATE INDEX education_index ON {os.getenv('DB_DATABASE')}.internship_candidates (education)')
        
        cursor.execute(f'SELECT name, birth_date, sex FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE education LIKE "Faculty of Design of Moscow State University"')
        result_with_index=cursor.fetchall()
        
        cursor.execute(f'DROP INDEX education_index ON {os.getenv('DB_DATABASE')}.internship_candidates')
        
        self.assertEqual(result_without_index, result_with_index)

    def test_education_index_Computer_Science_and_Control_Systems_of_Bauman_Moscow_State_Technical_University(self):
        
        cursor.execute(f'SELECT name, birth_date, sex FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE education LIKE "Computer Science and Control Systems%"')
        result_without_index=cursor.fetchall()
        
        cursor.execute(f'CREATE INDEX education_index ON {os.getenv('DB_DATABASE')}.internship_candidates (education)')
        
        cursor.execute(f'SELECT name, birth_date, sex FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE education LIKE "Computer Science and Control Systems%"')
        result_with_index=cursor.fetchall()
        
        cursor.execute(f'DROP INDEX education_index ON {os.getenv('DB_DATABASE')}.internship_candidates')
        
        self.assertEqual(result_without_index, result_with_index)
        
    def test_education_index_Oxford_Faculty_of_Business(self):
        
        cursor.execute(f'SELECT name, birth_date, sex FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE education LIKE "Oxford Faculty of Business"')
        result_without_index=cursor.fetchall()
        
        cursor.execute(f'CREATE INDEX education_index ON {os.getenv('DB_DATABASE')}.internship_candidates (education)')
        
        cursor.execute(f'SELECT name, birth_date, sex FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE education LIKE "Oxford Faculty of Business"')
        result_with_index=cursor.fetchall()
        
        cursor.execute(f'DROP INDEX education_index ON {os.getenv('DB_DATABASE')}.internship_candidates')
        
        self.assertEqual(result_without_index, result_with_index)
        
    def test_education_index_Faculty_of_Graphics_of_the_Stroganov_Moscow_State_Academy_of_Art_and_Industry(self):
        
        cursor.execute(f'SELECT name, birth_date, sex FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE education LIKE "Faculty of Graphics%"')
        result_without_index=cursor.fetchall()
        
        cursor.execute(f'CREATE INDEX education_index ON {os.getenv('DB_DATABASE')}.internship_candidates (education)')
        
        cursor.execute(f'SELECT name, birth_date, sex FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE education LIKE "Faculty of Graphics%"')
        result_with_index=cursor.fetchall()
        
        cursor.execute(f'DROP INDEX education_index ON {os.getenv('DB_DATABASE')}.internship_candidates')
        
        self.assertEqual(result_without_index, result_with_index)
        
        
class SelectPerfTest(unittest.TestCase):
    
    def test_name_index_Nikolaeva_Violetta(self):
        
        time_select_without_index_mas = [] 
        
        for i in range(10):
            time_select_without_index = datetime.datetime.now()
            cursor.execute(f'SELECT birth_date, sex, education FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE name LIKE "Nikolaeva Violetta"')
            time_select_without_index = datetime.datetime.now() - time_select_without_index
            result_without_index=cursor.fetchall()
            time_select_without_index_mas.append(time_select_without_index)
            
        cursor.execute(f'CREATE INDEX name_index ON {os.getenv('DB_DATABASE')}.internship_candidates (name)')
        connection.commit()
        
        time_select_with_index_mas = [] 
        
        for i in range(10):
            start_select_with_index = datetime.datetime.now()
            cursor.execute(f'SELECT birth_date, sex, education FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE name LIKE "Nikolaeva Violetta"')
            time_select_with_index = datetime.datetime.now() - start_select_with_index
            result_with_index=cursor.fetchall()
            time_select_with_index_mas.append(time_select_with_index)
            
        cursor.execute(f'DROP INDEX name_index ON {os.getenv('DB_DATABASE')}.internship_candidates')
        connection.commit()
                
        time_win_count = 0
        for i in range(10):
            if time_select_with_index_mas[i] < time_select_without_index_mas[i]:
                time_win_count += 1
        self.assertEqual(time_win_count > 5, 1)

    def test_name_index_Nikolaev_Georgy(self):
        
        time_select_without_index_mas = [] 
        
        for i in range(10):
            time_select_without_index = datetime.datetime.now()
            cursor.execute(f'SELECT birth_date, sex, education FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE name LIKE "Nikolaev Georgy"')
            time_select_without_index = datetime.datetime.now() - time_select_without_index
            result_without_index=cursor.fetchall()
            time_select_without_index_mas.append(time_select_without_index)
            
        cursor.execute(f'CREATE INDEX name_index ON {os.getenv('DB_DATABASE')}.internship_candidates (name)')
        connection.commit()
        
        time_select_with_index_mas = [] 
        
        for i in range(10):
            start_select_with_index = datetime.datetime.now()
            cursor.execute(f'SELECT birth_date, sex, education FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE name LIKE "Nikolaev Georgy"')
            time_select_with_index = datetime.datetime.now() - start_select_with_index
            result_with_index=cursor.fetchall()
            time_select_with_index_mas.append(time_select_with_index)
            
        cursor.execute(f'DROP INDEX name_index ON {os.getenv('DB_DATABASE')}.internship_candidates')
        connection.commit()
                
        time_win_count = 0
        for i in range(10):
            if time_select_with_index_mas[i] < time_select_without_index_mas[i]:
                time_win_count += 1
        self.assertEqual(time_win_count > 5, 1)
    
    def test_name_index_Tkacheva_Vera(self):
        
        time_select_without_index_mas = [] 
        
        for i in range(10):
            time_select_without_index = datetime.datetime.now()
            cursor.execute(f'SELECT birth_date, sex, education FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE name LIKE "Tkacheva Vera"')
            time_select_without_index = datetime.datetime.now() - time_select_without_index
            result_without_index=cursor.fetchall()
            time_select_without_index_mas.append(time_select_without_index)
            
        cursor.execute(f'CREATE INDEX name_index ON {os.getenv('DB_DATABASE')}.internship_candidates (name)')
        connection.commit()
        
        time_select_with_index_mas = [] 
        
        for i in range(10):
            start_select_with_index = datetime.datetime.now()
            cursor.execute(f'SELECT birth_date, sex, education FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE name LIKE "Tkacheva Vera"')
            time_select_with_index = datetime.datetime.now() - start_select_with_index
            result_with_index=cursor.fetchall()
            time_select_with_index_mas.append(time_select_with_index)
            
        cursor.execute(f'DROP INDEX name_index ON {os.getenv('DB_DATABASE')}.internship_candidates')
        connection.commit()
                
        time_win_count = 0
        for i in range(10):
            if time_select_with_index_mas[i] < time_select_without_index_mas[i]:
                time_win_count += 1
        self.assertEqual(time_win_count > 5, 1)
    
    def test_name_index_Ovodov_Dmitry(self):
        
        time_select_without_index_mas = [] 
        
        for i in range(10):
            time_select_without_index = datetime.datetime.now()
            cursor.execute(f'SELECT birth_date, sex, education FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE name LIKE "Ovodov Dmitry"')
            time_select_without_index = datetime.datetime.now() - time_select_without_index
            result_without_index=cursor.fetchall()
            time_select_without_index_mas.append(time_select_without_index)
            
        cursor.execute(f'CREATE INDEX name_index ON {os.getenv('DB_DATABASE')}.internship_candidates (name)')
        connection.commit()
        
        time_select_with_index_mas = [] 
        
        for i in range(10):
            start_select_with_index = datetime.datetime.now()
            cursor.execute(f'SELECT birth_date, sex, education FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE name LIKE "Ovodov Dmitry"')
            time_select_with_index = datetime.datetime.now() - start_select_with_index
            result_with_index=cursor.fetchall()
            time_select_with_index_mas.append(time_select_with_index)
            
        cursor.execute(f'DROP INDEX name_index ON {os.getenv('DB_DATABASE')}.internship_candidates')
        connection.commit()
                
        time_win_count = 0
        for i in range(10):
            if time_select_with_index_mas[i] < time_select_without_index_mas[i]:
                time_win_count += 1
        self.assertEqual(time_win_count > 5, 1)
    
    def test_name_index_Savkina_Polina(self):
        
        time_select_without_index_mas = [] 
        
        for i in range(10):
            time_select_without_index = datetime.datetime.now()
            cursor.execute(f'SELECT birth_date, sex, education FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE name LIKE "Savkina Polina"')
            time_select_without_index = datetime.datetime.now() - time_select_without_index
            result_without_index=cursor.fetchall()
            time_select_without_index_mas.append(time_select_without_index)
            
        cursor.execute(f'CREATE INDEX name_index ON {os.getenv('DB_DATABASE')}.internship_candidates (name)')
        connection.commit()
        
        time_select_with_index_mas = [] 
        
        for i in range(10):
            start_select_with_index = datetime.datetime.now()
            cursor.execute(f'SELECT birth_date, sex, education FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE name LIKE "Savkina Polina"')
            time_select_with_index = datetime.datetime.now() - start_select_with_index
            result_with_index=cursor.fetchall()
            time_select_with_index_mas.append(time_select_with_index)
            
        cursor.execute(f'DROP INDEX name_index ON {os.getenv('DB_DATABASE')}.internship_candidates')
        connection.commit()
                
        time_win_count = 0
        for i in range(10):
            if time_select_with_index_mas[i] < time_select_without_index_mas[i]:
                time_win_count += 1
        self.assertEqual(time_win_count > 5, 1)
    
    def test_name_index_Zmaeva_Svetlana(self):
        
        time_select_without_index_mas = [] 
        
        for i in range(10):
            time_select_without_index = datetime.datetime.now()
            cursor.execute(f'SELECT birth_date, sex, education FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE name LIKE "Zmaeva Svetlana"')
            time_select_without_index = datetime.datetime.now() - time_select_without_index
            result_without_index=cursor.fetchall()
            time_select_without_index_mas.append(time_select_without_index)
            
        cursor.execute(f'CREATE INDEX name_index ON {os.getenv('DB_DATABASE')}.internship_candidates (name)')
        connection.commit()
        
        time_select_with_index_mas = [] 
        
        for i in range(10):
            start_select_with_index = datetime.datetime.now()
            cursor.execute(f'SELECT birth_date, sex, education FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE name LIKE "Zmaeva Svetlana"')
            time_select_with_index = datetime.datetime.now() - start_select_with_index
            result_with_index=cursor.fetchall()
            time_select_with_index_mas.append(time_select_with_index)
            
        cursor.execute(f'DROP INDEX name_index ON {os.getenv('DB_DATABASE')}.internship_candidates')
        connection.commit()
                
        time_win_count = 0
        for i in range(10):
            if time_select_with_index_mas[i] < time_select_without_index_mas[i]:
                time_win_count += 1
        self.assertEqual(time_win_count > 5, 1)
    
    def test_name_index_Zaitsev_Fedor(self):
        
        time_select_without_index_mas = [] 
        
        for i in range(10):
            time_select_without_index = datetime.datetime.now()
            cursor.execute(f'SELECT birth_date, sex, education FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE name LIKE "Zaitsev Fedor"')
            time_select_without_index = datetime.datetime.now() - time_select_without_index
            result_without_index=cursor.fetchall()
            time_select_without_index_mas.append(time_select_without_index)
            
        cursor.execute(f'CREATE INDEX name_index ON {os.getenv('DB_DATABASE')}.internship_candidates (name)')
        connection.commit()
        
        time_select_with_index_mas = [] 
        
        for i in range(10):
            start_select_with_index = datetime.datetime.now()
            cursor.execute(f'SELECT birth_date, sex, education FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE name LIKE "Zaitsev Fedor"')
            time_select_with_index = datetime.datetime.now() - start_select_with_index
            result_with_index=cursor.fetchall()
            time_select_with_index_mas.append(time_select_with_index)
            
        cursor.execute(f'DROP INDEX name_index ON {os.getenv('DB_DATABASE')}.internship_candidates')
        connection.commit()
                
        time_win_count = 0
        for i in range(10):
            if time_select_with_index_mas[i] < time_select_without_index_mas[i]:
                time_win_count += 1
        self.assertEqual(time_win_count > 5, 1)
    
    def test_name_index_Ivanov_Maxim(self):
        
        time_select_without_index_mas = [] 
        
        for i in range(10):
            time_select_without_index = datetime.datetime.now()
            cursor.execute(f'SELECT birth_date, sex, education FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE name LIKE "Ivanov Maxim"')
            time_select_without_index = datetime.datetime.now() - time_select_without_index
            result_without_index=cursor.fetchall()
            time_select_without_index_mas.append(time_select_without_index)
            
        cursor.execute(f'CREATE INDEX name_index ON {os.getenv('DB_DATABASE')}.internship_candidates (name)')
        connection.commit()
        
        time_select_with_index_mas = [] 
        
        for i in range(10):
            start_select_with_index = datetime.datetime.now()
            cursor.execute(f'SELECT birth_date, sex, education FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE name LIKE "Ivanov Maxim"')
            time_select_with_index = datetime.datetime.now() - start_select_with_index
            result_with_index=cursor.fetchall()
            time_select_with_index_mas.append(time_select_with_index)
            
        cursor.execute(f'DROP INDEX name_index ON {os.getenv('DB_DATABASE')}.internship_candidates')
        connection.commit()
                
        time_win_count = 0
        for i in range(10):
            if time_select_with_index_mas[i] < time_select_without_index_mas[i]:
                time_win_count += 1
        self.assertEqual(time_win_count > 5, 1)
    
    def test_name_index_Sophronova_Evgenia(self):
        
        time_select_without_index_mas = [] 
        
        for i in range(10):
            time_select_without_index = datetime.datetime.now()
            cursor.execute(f'SELECT birth_date, sex, education FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE name LIKE "Sophronova Evgenia"')
            time_select_without_index = datetime.datetime.now() - time_select_without_index
            result_without_index=cursor.fetchall()
            time_select_without_index_mas.append(time_select_without_index)
            
        cursor.execute(f'CREATE INDEX name_index ON {os.getenv('DB_DATABASE')}.internship_candidates (name)')
        connection.commit()
        
        time_select_with_index_mas = [] 
        
        for i in range(10):
            start_select_with_index = datetime.datetime.now()
            cursor.execute(f'SELECT birth_date, sex, education FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE name LIKE "Sophronova Evgenia"')
            time_select_with_index = datetime.datetime.now() - start_select_with_index
            result_with_index=cursor.fetchall()
            time_select_with_index_mas.append(time_select_with_index)
            
        cursor.execute(f'DROP INDEX name_index ON {os.getenv('DB_DATABASE')}.internship_candidates')
        connection.commit()
                
        time_win_count = 0
        for i in range(10):
            if time_select_with_index_mas[i] < time_select_without_index_mas[i]:
                time_win_count += 1
        self.assertEqual(time_win_count > 5, 1)
    
    def test_education_index_Information_Security_of_the_Higher_School_of_Economics(self):
        
        time_select_without_index_mas = [] 
        
        for i in range(10):
            time_select_without_index = datetime.datetime.now()
            cursor.execute(f'SELECT name, birth_date, sex FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE education LIKE "Information Security%"')
            time_select_without_index = datetime.datetime.now() - time_select_without_index
            result_without_index=cursor.fetchall()
            time_select_without_index_mas.append(time_select_without_index)
            
        cursor.execute(f'CREATE INDEX education_index ON {os.getenv('DB_DATABASE')}.internship_candidates (education)')
        connection.commit()
        
        time_select_with_index_mas = [] 
        
        for i in range(10):
            start_select_with_index = datetime.datetime.now()
            cursor.execute(f'SELECT name, birth_date, sex FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE education LIKE "Information Security%"')
            time_select_with_index = datetime.datetime.now() - start_select_with_index
            result_with_index=cursor.fetchall()
            time_select_with_index_mas.append(time_select_with_index)
                
        cursor.execute(f'DROP INDEX education_index ON {os.getenv('DB_DATABASE')}.internship_candidates')
        connection.commit()
                
        time_win_count = 0
        for i in range(10):
            if time_select_with_index_mas[i] < time_select_without_index_mas[i]:
                time_win_count += 1
        self.assertEqual(time_win_count > 5, 1)
    
    def test_education_index_System_analysis_of_MEPhI(self):
        
        time_select_without_index_mas = [] 
        
        for i in range(10):
            time_select_without_index = datetime.datetime.now()
            cursor.execute(f'SELECT name, birth_date, sex FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE education LIKE "System analysis of MEPhI"')
            time_select_without_index = datetime.datetime.now() - time_select_without_index
            result_without_index=cursor.fetchall()
            time_select_without_index_mas.append(time_select_without_index)
            
        cursor.execute(f'CREATE INDEX education_index ON {os.getenv('DB_DATABASE')}.internship_candidates (education)')
        connection.commit()
        
        time_select_with_index_mas = [] 
        
        for i in range(10):
            start_select_with_index = datetime.datetime.now()
            cursor.execute(f'SELECT name, birth_date, sex FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE education LIKE "System analysis of MEPhI"')
            time_select_with_index = datetime.datetime.now() - start_select_with_index
            result_with_index=cursor.fetchall()
            time_select_with_index_mas.append(time_select_with_index)
                
        cursor.execute(f'DROP INDEX education_index ON {os.getenv('DB_DATABASE')}.internship_candidates')
        connection.commit()
                
        time_win_count = 0
        for i in range(10):
            if time_select_with_index_mas[i] < time_select_without_index_mas[i]:
                time_win_count += 1
        self.assertEqual(time_win_count > 5, 1)
        
    def test_education_index_HSE_Business_Informatics(self):
        
        time_select_without_index_mas = [] 
        
        for i in range(10):
            time_select_without_index = datetime.datetime.now()
            cursor.execute(f'SELECT name, birth_date, sex FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE education LIKE "HSE Business Informatics"')
            time_select_without_index = datetime.datetime.now() - time_select_without_index
            result_without_index=cursor.fetchall()
            time_select_without_index_mas.append(time_select_without_index)
            
        cursor.execute(f'CREATE INDEX education_index ON {os.getenv('DB_DATABASE')}.internship_candidates (education)')
        connection.commit()
        
        time_select_with_index_mas = [] 
        
        for i in range(10):
            start_select_with_index = datetime.datetime.now()
            cursor.execute(f'SELECT name, birth_date, sex FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE education LIKE "HSE Business Informatics"')
            time_select_with_index = datetime.datetime.now() - start_select_with_index
            result_with_index=cursor.fetchall()
            time_select_with_index_mas.append(time_select_with_index)
                
        cursor.execute(f'DROP INDEX education_index ON {os.getenv('DB_DATABASE')}.internship_candidates')
        connection.commit()
                
        time_win_count = 0
        for i in range(10):
            if time_select_with_index_mas[i] < time_select_without_index_mas[i]:
                time_win_count += 1
        self.assertEqual(time_win_count > 5, 1)
        
    def test_education_index_Moscow_State_University_Faculty_of_Law(self):
        
        time_select_without_index_mas = [] 
        
        for i in range(10):
            time_select_without_index = datetime.datetime.now()
            cursor.execute(f'SELECT name, birth_date, sex FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE education LIKE "Moscow State University Faculty of Law"')
            time_select_without_index = datetime.datetime.now() - time_select_without_index
            result_without_index=cursor.fetchall()
            time_select_without_index_mas.append(time_select_without_index)
            
        cursor.execute(f'CREATE INDEX education_index ON {os.getenv('DB_DATABASE')}.internship_candidates (education)')
        connection.commit()
        
        time_select_with_index_mas = [] 
        
        for i in range(10):
            start_select_with_index = datetime.datetime.now()
            cursor.execute(f'SELECT name, birth_date, sex FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE education LIKE "Moscow State University Faculty of Law"')
            time_select_with_index = datetime.datetime.now() - start_select_with_index
            result_with_index=cursor.fetchall()
            time_select_with_index_mas.append(time_select_with_index)
                
        cursor.execute(f'DROP INDEX education_index ON {os.getenv('DB_DATABASE')}.internship_candidates')
        connection.commit()
                
        time_win_count = 0
        for i in range(10):
            if time_select_with_index_mas[i] < time_select_without_index_mas[i]:
                time_win_count += 1
        self.assertEqual(time_win_count > 5, 1)
        
    def test_education_index_Faculty_of_Design_of_Moscow_State_University(self):
        
        time_select_without_index_mas = [] 
        
        for i in range(10):
            time_select_without_index = datetime.datetime.now()
            cursor.execute(f'SELECT name, birth_date, sex FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE education LIKE "Faculty of Design of Moscow State University"')
            time_select_without_index = datetime.datetime.now() - time_select_without_index
            result_without_index=cursor.fetchall()
            time_select_without_index_mas.append(time_select_without_index)
            
        cursor.execute(f'CREATE INDEX education_index ON {os.getenv('DB_DATABASE')}.internship_candidates (education)')
        connection.commit()
        
        time_select_with_index_mas = [] 
        
        for i in range(10):
            start_select_with_index = datetime.datetime.now()
            cursor.execute(f'SELECT name, birth_date, sex FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE education LIKE "Faculty of Design of Moscow State University"')
            time_select_with_index = datetime.datetime.now() - start_select_with_index
            result_with_index=cursor.fetchall()
            time_select_with_index_mas.append(time_select_with_index)
                
        cursor.execute(f'DROP INDEX education_index ON {os.getenv('DB_DATABASE')}.internship_candidates')
        connection.commit()
                
        time_win_count = 0
        for i in range(10):
            if time_select_with_index_mas[i] < time_select_without_index_mas[i]:
                time_win_count += 1
        self.assertEqual(time_win_count > 5, 1)

    def test_education_index_Computer_Science_and_Control_Systems_of_Bauman_Moscow_State_Technical_University(self):
        
        time_select_without_index_mas = [] 
        
        for i in range(10):
            time_select_without_index = datetime.datetime.now()
            cursor.execute(f'SELECT name, birth_date, sex FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE education LIKE "Computer Science and Control Systems%"')
            time_select_without_index = datetime.datetime.now() - time_select_without_index
            result_without_index=cursor.fetchall()
            time_select_without_index_mas.append(time_select_without_index)
            
        cursor.execute(f'CREATE INDEX education_index ON {os.getenv('DB_DATABASE')}.internship_candidates (education)')
        connection.commit()
        
        time_select_with_index_mas = [] 
        
        for i in range(10):
            start_select_with_index = datetime.datetime.now()
            cursor.execute(f'SELECT name, birth_date, sex FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE education LIKE "Computer Science and Control Systems%"')
            time_select_with_index = datetime.datetime.now() - start_select_with_index
            result_with_index=cursor.fetchall()
            time_select_with_index_mas.append(time_select_with_index)
                
        cursor.execute(f'DROP INDEX education_index ON {os.getenv('DB_DATABASE')}.internship_candidates')
        connection.commit()
                
        time_win_count = 0
        for i in range(10):
            if time_select_with_index_mas[i] < time_select_without_index_mas[i]:
                time_win_count += 1
        self.assertEqual(time_win_count > 5, 1)
        
    def test_education_index_Oxford_Faculty_of_Business(self):
        
        time_select_without_index_mas = [] 
        
        for i in range(10):
            time_select_without_index = datetime.datetime.now()
            cursor.execute(f'SELECT name, birth_date, sex FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE education LIKE "Oxford Faculty of Business"')
            time_select_without_index = datetime.datetime.now() - time_select_without_index
            result_without_index=cursor.fetchall()
            time_select_without_index_mas.append(time_select_without_index)
            
        cursor.execute(f'CREATE INDEX education_index ON {os.getenv('DB_DATABASE')}.internship_candidates (education)')
        connection.commit()
        
        time_select_with_index_mas = [] 
        
        for i in range(10):
            start_select_with_index = datetime.datetime.now()
            cursor.execute(f'SELECT name, birth_date, sex FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE education LIKE "Oxford Faculty of Business"')
            time_select_with_index = datetime.datetime.now() - start_select_with_index
            result_with_index=cursor.fetchall()
            time_select_with_index_mas.append(time_select_with_index)
                
        cursor.execute(f'DROP INDEX education_index ON {os.getenv('DB_DATABASE')}.internship_candidates')
        connection.commit()
                
        time_win_count = 0
        for i in range(10):
            if time_select_with_index_mas[i] < time_select_without_index_mas[i]:
                time_win_count += 1
        self.assertEqual(time_win_count > 5, 1)
        
    def test_education_index_Faculty_of_Graphics_of_the_Stroganov_Moscow_State_Academy_of_Art_and_Industry(self):
        
        time_select_without_index_mas = [] 
        
        for i in range(10):
            time_select_without_index = datetime.datetime.now()
            cursor.execute(f'SELECT name, birth_date, sex FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE education LIKE "Faculty of Graphics%"')
            time_select_without_index = datetime.datetime.now() - time_select_without_index
            result_without_index=cursor.fetchall()
            time_select_without_index_mas.append(time_select_without_index)
            
        cursor.execute(f'CREATE INDEX education_index ON {os.getenv('DB_DATABASE')}.internship_candidates (education)')
        connection.commit()
        
        time_select_with_index_mas = [] 
        
        for i in range(10):
            start_select_with_index = datetime.datetime.now()
            cursor.execute(f'SELECT name, birth_date, sex FROM {os.getenv('DB_DATABASE')}.internship_candidates WHERE education LIKE "Faculty of Graphics%"')
            time_select_with_index = datetime.datetime.now() - start_select_with_index
            result_with_index=cursor.fetchall()
            time_select_with_index_mas.append(time_select_with_index)
                
        cursor.execute(f'DROP INDEX education_index ON {os.getenv('DB_DATABASE')}.internship_candidates')
        connection.commit()
                
        time_win_count = 0
        for i in range(10):
            if time_select_with_index_mas[i] < time_select_without_index_mas[i]:
                time_win_count += 1
        self.assertEqual(time_win_count > 5, 1)

if __name__ == "__main__":
    unittest.main()
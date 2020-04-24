# import logging #https://python-scripts.com/logging-python https://dev-gang.ru/article/modul-logging-v-python-sugk5e4d8u/
import psycopg2
import os
import datetime

from structure import load_structure
from events import load_events
from data_mart import add_data_marts

a = 5
b = 0

# try:
#   c = a / b
# except Exception as e:
#   logging.error("Exception occurred", exc_info=True)
#
# try:
#   c = a / b
# except Exception as e:
#   logging.exception("Exception occurred")


def tm():
    return datetime.datetime.now().isoformat()


folder = 'E:\\Coursera_zips'
conn_settings = {
    'dbname': 'test',
    'user': 'postgres',
    'password': 'pg215',
    'host': 'VM-AS494',
}

zip_list = list(filter(lambda x: x[-4:] == '.zip', os.listdir(folder)))

for zipname in zip_list:
    conn = psycopg2.connect(**conn_settings)


    # загрузим структуры и события по каждому архиву из папки, если еще не загружены
    try:
        print('\n***************** loading structure *************************')
        step = '|structure|'
        course_id, load_id = load_structure(folder, zipname, conn)

        print('\n***************** loading events *************************')
        step = '|events|'
        load_events(folder, zipname, conn, course_id, load_id, conn_settings)

        conn.commit()

        conn = psycopg2.connect(**conn_settings)
        print('\n***************** creating data marts *************************')
        step = '|data-mart|'
        add_data_marts(conn, load_id, course_id)
        conn.commit()
    except Exception as e:
        print(zipname, step, 'some problem occured')
        print(e.__repr__())

    print(tm())



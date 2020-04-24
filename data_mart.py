import datetime

def tm():
    return datetime.datetime.now().isoformat()

def add_data_marts(conn, load_id, course_id):
    print('load_id: ', load_id)
    cursor = conn.cursor()
    print(tm())

    # cursor.execute("DROP INDEX if exists coursera_event.caq_use_ass_actstartts")
    # sql = '''CREATE INDEX caq_use_ass_actstartts
    # ON coursera_event.caq_event USING btree
    # (hse_user_ext_id COLLATE pg_catalog."default" ASC NULLS LAST
    # , assessment_ext_id COLLATE pg_catalog."default" ASC NULLS LAST
    # , action_start_ts ASC NULLS LAST)
    # TABLESPACE pg_default
    # where course_id = {}'''.format(course_id)
    # cursor.execute(sql)
    # print(tm(), 'index created: caq_use_ass_actstartts')
    #
    # cursor.execute("DROP INDEX if exists coursera_event.cqo_course_response_option")
    # sql = '''CREATE INDEX cqo_course_response_option
    # ON coursera_event.cqo_event USING btree
    # (course_id ASC NULLS LAST
    #  , response_ext_id COLLATE pg_catalog."default" ASC NULLS LAST
    #  , option_ext_id COLLATE pg_catalog."default" ASC NULLS LAST)
    # TABLESPACE pg_default
    # where course_id = {}'''.format(course_id)
    # cursor.execute(sql)
    # print(tm(), 'index created: cqo_course_response_option')

    cursor.callproc("data_mart.app0_ins", [load_id])
    for it in cursor.fetchone()[0].split('*'):
        print(it)
    #cursor.close()

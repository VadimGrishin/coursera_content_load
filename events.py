import zipfile
import datetime
import inspect
import psycopg2

# скрипты временных буферов для быстрой загрузки (курсера)
on_demand_sessions_script = '''
        CREATE TABLE on_demand_sessions (
            course_id VARCHAR(50)
            ,on_demand_session_id VARCHAR(50)
            ,on_demand_sessions_start_ts TIMESTAMP
            ,on_demand_sessions_end_ts TIMESTAMP
            ,on_demand_sessions_enrollment_end_ts TIMESTAMP
            ,course_branch_id VARCHAR(50)
        );
        '''

assessment_actions_script = '''
        CREATE TABLE assessment_actions (
            assessment_action_id VARCHAR(50)
            ,assessment_action_base_id VARCHAR(50)
            ,assessment_id VARCHAR(50)
            ,assessment_scope_id VARCHAR(300)
            ,assessment_scope_type_id INT4
            ,assessment_action_version INT4
            ,assessment_action_ts TIMESTAMP
            ,assessment_action_start_ts TIMESTAMP
            ,guest_user_id VARCHAR(50)
            ,hse_user_id VARCHAR(50) NOT NULL
        );
        '''

assessment_responses_script = '''
        CREATE TABLE assessment_responses (
            assessment_response_id VARCHAR(50)
            ,assessment_id VARCHAR(50)
            ,assessment_action_id VARCHAR(50)
            ,assessment_action_version INT4
            ,assessment_question_id VARCHAR(50)
            ,assessment_response_score FLOAT4
            ,assessment_response_weighted_score FLOAT4
        );
        '''
assessment_response_options_script = '''
        CREATE TABLE assessment_response_options (
            assessment_response_id VARCHAR(50)
            ,assessment_option_id VARCHAR(50)
            ,assessment_response_correct BOOL
            ,assessment_response_feedback VARCHAR(20000)
            ,assessment_response_selected BOOL
        );
        '''
on_demand_session_memberships_script = '''
        CREATE TABLE on_demand_session_memberships (
            course_id VARCHAR(50)
            ,on_demand_session_id VARCHAR(50)
            ,hse_user_id VARCHAR(50) NOT NULL
            ,on_demand_sessions_membership_start_ts TIMESTAMP
            ,on_demand_sessions_membership_end_ts TIMESTAMP
        );
        '''

# скрипты загрузки из буферов в логи событий (курсера)
csess_event_script = '''
        insert into coursera_event.csess_event
          (select *,  {} course_id
          from on_demand_sessions);
        '''

caq_event_script = '''
        INSERT INTO coursera_event.caq_event
            (SELECT
              {} course_id 
              , aa.hse_user_id
              , aa.assessment_id
              , aa.assessment_action_id
              , aa.assessment_action_base_id
              , aa.assessment_action_version
              , CAST(aa.assessment_action_ts AS TIMESTAMP)
              , CAST(aa.assessment_action_start_ts AS TIMESTAMP)
              , ar.assessment_question_id
              , ar.assessment_response_score
              , ar.assessment_response_weighted_score
              , ar.assessment_response_id
            FROM assessment_actions aa
              LEFT  JOIN assessment_responses ar USING (assessment_action_id));
        '''

cqo_event_script = """
        INSERT INTO coursera_event.cqo_event 
            (SELECT DISTINCT
              assessment_response_id
              , case 
                  when substring(assessment_option_id from 1 for 2)='0.' then  substring(assessment_option_id from 1 for 10) 
                  else assessment_option_id
              end
              , cast(assessment_response_correct as bool)
              , cast(assessment_response_selected as bool)
              , {} course_id 
            FROM assessment_response_options);
        """

sessmemb_event_script = '''
        insert into coursera_event.sessmemb_event
            (select *,  {} course_id
              from on_demand_session_memberships);
        '''

event_dict = {
    'csess_event': {
        'source': [
            {'name': 'on_demand_sessions',
             'script': on_demand_sessions_script
             }
        ],
        'dest_script': csess_event_script
    },

    'caq_event': {
        'source': [
            {
                'name': 'assessment_actions',
                'script': assessment_actions_script
            },
            {
                'name': 'assessment_responses',
                'script': assessment_responses_script
            },
        ],
        'dest_script': caq_event_script
    },

    'cqo_event': {
        'source': [
            {'name': 'assessment_response_options',
             'script': assessment_response_options_script
             }
        ],
        'dest_script': cqo_event_script
    },

    'sessmemb_event': {
        'source': [
            {'name': 'on_demand_session_memberships',
             'script': on_demand_session_memberships_script
             }
        ],
        'dest_script': sessmemb_event_script
    }
}


def tm():
    return datetime.datetime.now().isoformat()


def copy_tmp(myzip, cursor, tmp_name):
    """
    fill table from csv to DB (fast insert)
    """
    print(tm(), inspect.stack()[0][3], locals())
    copy_sql = f"""
    copy {tmp_name}
    from stdin with
     csv
     header
     delimiter as ','
     escape '\\'
    """
    csv_name = f'{tmp_name}.csv'
    with myzip.open(csv_name) as from_archive:
        cursor.copy_expert(sql=copy_sql, file=from_archive)


def create_tmp(myzip, event_source, conn_settings):
    """
    создать и заполнить буферы из курсеровских csv-таблиц для одного event
    """
    print(tm(), inspect.stack()[0][3], locals())

    for it in event_source:
        conn_tmp = psycopg2.connect(**conn_settings)
        cursor = conn_tmp.cursor()
        script = f"drop table if exists {it['name']};\n{it['script']}"
        cursor.execute(script)
        copy_tmp(myzip, cursor, it['name'])
        print(tm(), 'one source added')
        conn_tmp.commit()

def check_event(conn, load_id, dest_name):
    """
    Проверяет наличие загрузки в базе (метки в coursera_event.event)
    """
    print(tm(), inspect.stack()[0][3], locals())
    cursor = conn.cursor()
    cursor.execute(f"select id from coursera_event.event where load_id={load_id} and dest_name='{dest_name}'")

    event_id = cursor.fetchone()
    if event_id:
        print(f'Событие {dest_name} для курса загрузки {load_id} уже в базе')
        return True   # True - already exists
    cursor.close()

    return False


def load_events(folder, zipname, conn, course_id, load_id, conn_settings):
    """
    загрузка в событийные логи (обертка)
    """
    cursor = conn.cursor()

    print(tm(), inspect.stack()[0][3], locals())

    cursor.execute('DROP INDEX if exists coursera_event.cqo_course_response_option')

    myzip = zipfile.ZipFile(f'{folder}/{zipname}')

    for event in event_dict:

        if not check_event(conn, load_id, event):
            create_tmp(myzip, event_dict[event]['source'], conn_settings)

            cursor.execute(event_dict[event]['dest_script'].format(course_id))
            print(tm(), 'dest added')
            cursor.execute(
                f"insert into coursera_event.event (dest_name, load_ts, load_id) values('{event}', now(), {load_id})")

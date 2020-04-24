from sqlalchemy import create_engine
import pandas as pd
import zipfile
import datetime
import inspect


def convert4null(var):
    """
    служебная функция для промежуточного преобразования Nan в null
    """
    if pd.isna(var):
        return '#@$'

    return var


def fill_item_type(myzip, engine, cur):
    """
    upsert справочника:
    DB.item_type  <- course_item_types.csv
    """
    print(tm(), inspect.stack()[0][3], locals())
    with myzip.open('course_item_types.csv') as from_archive:
        df_item_type = pd.read_csv(from_archive)

    print(len(df_item_type))

    df_item_type = df_item_type.rename(columns={'course_item_type_id': 'id', 'course_item_type_desc': 'descr',
                                                'course_item_type_category': 'categ',
                                                'course_item_type_graded': 'graded'})
    df_item_type.drop(['atom_content_type_id'], axis='columns', inplace=True)

    # пишем во временную таблицу
    df_item_type.to_sql('tmp', engine, if_exists='replace')

    # upsert из временной таблицы в item_type
    sql = 'INSERT INTO coursera_structure.item_type \
    (select id, descr, categ, cast(graded as boolean) from tmp) \
    ON CONFLICT (id) DO NOTHING'
    cur.execute(sql)


def fill_assm_type(myzip, engine, cur):
    """
    upsert справочника:
    assessment_type <- assessment_types.csv
    """
    print(tm(), inspect.stack()[0][3], locals())
    with myzip.open('assessment_types.csv') as from_archive:
        df_assmm_type = pd.read_csv(from_archive)

    print(len(df_assmm_type))

    df_assmm_type = df_assmm_type.rename(columns={'assessment_type_id': 'id', 'assessment_type_desc': 'descr'})

    # пишем во временную таблицу
    df_assmm_type.to_sql('tmp1', engine, if_exists='replace')

    # upsert из временной таблицы в assessment_type
    sql = 'INSERT INTO coursera_structure.assessment_type \
    (select id, descr from tmp1) \
    ON CONFLICT (id) DO NOTHING'
    cur.execute(sql)


def fill_que_type(myzip, engine, cur):
    """
    upsert справочника:
    question_type <- assessment_question_types.csv
    """
    print(tm(), inspect.stack()[0][3], locals())
    with myzip.open('assessment_question_types.csv') as from_archive:
        df_assmm_type = pd.read_csv(from_archive)

    print(len(df_assmm_type))

    df_assmm_type = df_assmm_type.rename(
        columns={'assessment_question_type_id': 'id', 'assessment_question_type_desc': 'descr'})

    # пишем во временную таблицу
    df_assmm_type.to_sql('tmp2', engine, if_exists='replace')

    # upsert из временной таблицы в assessment_type
    sql = 'INSERT INTO coursera_structure.question_type \
    (select id, descr from tmp2) \
    ON CONFLICT (id) DO NOTHING'
    cur.execute(sql)


def upsert_reference_tables(myzip):
    """
    обертка для обновления справочников
    """
    print(tm(), inspect.stack()[0][3], locals())
    engine = create_engine('postgresql+psycopg2://postgres:pg215@VM-AS494:5432/test')

    conn = engine.raw_connection()
    cur = conn.cursor()

    fill_item_type(myzip, engine, cur)
    fill_assm_type(myzip, engine, cur)
    fill_que_type(myzip, engine, cur)

    conn.commit()


def add_load(zipname, cursor):
    """
    Проверяет наличие загрузки в базе и при отсутствии - добавляет запись в coursera_structure.load
    """
    print(tm(), inspect.stack()[0][3], locals())
    cursor.execute(f"select id from coursera_structure.load where name='{zipname[:-4]}'")
    load_id = cursor.fetchone()

    if load_id:
        load_id = load_id[0]
        print(f'Срез курса {zipname} уже в базе')
        cursor.execute(f"select id from coursera_structure.course where load_id={load_id}")
        course_id = cursor.fetchone()[0]
        return course_id, load_id   # course_id already exists
    else:
        cursor.execute(
            f"insert into coursera_structure.load (name, load_ts) values('{zipname[:-4]}', now()) returning id")
        return False, cursor.fetchone()[0]


def add_course(myzip, load_id, conn, cursor):
    """
    DB.course  | courses.csv
    ----------------------------
    id  -  (series nextval)
    ext_id  <-   course_id
    slug    <-   course_slug
    name    <-   course_name
    descr   <-   course_desc
    load_id - current load.id
    """
    print(tm(), inspect.stack()[0][3], locals())
    with myzip.open('courses.csv') as from_archive:
        df = pd.read_csv(from_archive, escapechar='\\')
    #print(df)

    for index, row in df.iterrows():
        ext_id = row['course_id']
        slug = row['course_slug']
        name = row['course_name'].replace("'", "''")
        desc = convert4null(row['course_desc']).replace("'", "''")

        course_tmpdf = pd.read_sql_query(
            f"select * from coursera_structure.course where slug='{slug}' and ext_id is null", conn)

        # поскольку слаги всех курсов были залиты в базу заранее:
        if course_tmpdf.shape[0] > 0:
            # заполнить остальные поля в текущей строке, если слаг еще не "привлекался"
            id_ = course_tmpdf['id'].values[0]
            sql = f"update coursera_structure.course \
             set (ext_id, name, descr, load_id) = ('{ext_id}', '{name}', '{desc}', {load_id}) \
             where id={id_} returning id"
        else:
            # если нет слага с пустым курсом, добавить новый курс
            sql = f"insert into coursera_structure.course (ext_id, slug, name, descr, load_id) \
                           values ('{ext_id}', '{slug}', '{name}', '{desc}', {load_id}) returning id"

        #print(sql)
        cursor.execute(sql)

    return cursor.fetchone()[0]


def add_branch(myzip, load_id, conn, cursor):
    """
    DB.branch   | course_branches.csv
    ----------------------------
    id          - (series nextval)
    course_id   - (FK from course.id) <- course_id
    created_ts  <- authoring_course_branch_created_ts
    ext_id      <- course_branch_id
    load_id     - (current load.id)
    """
    print(tm(), inspect.stack()[0][3], locals())
    with myzip.open('course_branches.csv') as from_archive:
        df = pd.read_csv(from_archive, parse_dates=['authoring_course_branch_created_ts'], escapechar='\\')

    course_ext_id = ''
    for index, row in df.iterrows():
        # course.id Postgres'а для внешнего ключа:
        if course_ext_id != row['course_id']:  # в данном случае лишнее, но на будущее
            course_ext_id = row['course_id']
            course_sql = f"select id from coursera_structure.course where load_id={load_id} \
                and ext_id='{course_ext_id}'"
            course_id = pd.read_sql_query(course_sql, conn)['id'].values[0]
            bname = row['authoring_course_branch_name']

        created_ts = convert4null(row['authoring_course_branch_created_ts'])
        ext_id = row['course_branch_id']

        sql = f"insert into coursera_structure.branch (course_id, ext_id, name, created_ts, load_id) \
                        values ({course_id}, '{ext_id}','{bname}', '{created_ts}', {load_id})".replace("'#@$'", "null")

        cursor.execute(sql)

    return 0


def add_module(myzip, load_id, conn, cursor):
    """
    DB.module     | course_branch_modules.csv
    ----------------------------
    id            - (series nextval)
    branch_id     - (FK from branch.id)  <- course_branch_id
    branch_ext_id <- course_branch_id
    ext_id        <- course_module_id
    ord           <- course_branch_module_order
    name          <- course_branch_module_name
    descr         <- course_branch_module_desc
    load_id       - (current load.id)
    """
    print(tm(), inspect.stack()[0][3], locals())
    with myzip.open('course_branch_modules.csv') as from_archive:
        df = pd.read_csv(from_archive, escapechar='\\')
    df = df.sort_values(by=['course_branch_id'])

    branch_ext_id = ''
    for index, row in df.iterrows():
        # branch.id Postgres'а для внешнего ключа:
        if branch_ext_id != row['course_branch_id']:
            branch_ext_id = row['course_branch_id']
            branch_sql = f"select id from coursera_structure.branch where load_id={load_id} \
                and ext_id='{branch_ext_id}'"
            branch_id = pd.read_sql_query(branch_sql, conn)['id'].values[0]

        ext_id = row['course_module_id']
        ord_ = row['course_branch_module_order']
        name = row['course_branch_module_name'].replace("'", "''")
        descr = convert4null(row['course_branch_module_desc']).replace("'", "''")

        sql = f"insert into coursera_structure.module (branch_id, branch_ext_id, ext_id, ord, name, descr, load_id) \
                        values ({branch_id}, '{branch_ext_id}', '{ext_id}', {ord_}, '{name}', '{descr}', {load_id})"

        cursor.execute(sql)

    return 0


def add_lesson(myzip, load_id, conn, cursor):
    """
    DB.lesson     | course_branch_lessons.csv
    ----------------------------
    id            - (series nextval)
    module_id  - (FK from module.id) <- course_branch_id, course_module_id
    branch_ext_id <- course_branch_id
    ext_id        <- course_lesson_id
    ord           <- course_branch_lesson_order
    name          <- course_branch_lesson_name
    load_id       - (current load.id)
    """
    print(tm(), inspect.stack()[0][3], locals())
    with myzip.open('course_branch_lessons.csv') as from_archive:
        df = pd.read_csv(from_archive, escapechar='\\')
    df = df.sort_values(by=['course_branch_id', 'course_module_id'])

    branch_ext_id = ''
    module_ext_id = ''
    for index, row in df.iterrows():
        # module.id Postgres'а для внешнего ключа:
        if branch_ext_id != row['course_branch_id'] or module_ext_id != row['course_module_id']:
            branch_ext_id = row['course_branch_id']
            module_ext_id = row['course_module_id']
            module_sql = f"select id from coursera_structure.module where load_id={load_id} and\
             branch_ext_id='{branch_ext_id}' and ext_id='{module_ext_id}'"
            module_id = pd.read_sql_query(module_sql, conn)['id'].values[0]

        ext_id = row['course_lesson_id']
        ord_ = row['course_branch_lesson_order']
        name = row['course_branch_lesson_name'].replace("'", "''")

        sql = f"insert into coursera_structure.lesson (module_id, branch_ext_id, ext_id, ord, name, load_id) \
                        values ({module_id}, '{branch_ext_id}', '{ext_id}', {ord_}, '{name}', {load_id})"

        cursor.execute(sql)

    return 0


def add_item(myzip, load_id, conn, cursor):
    """
    DB.item       | course_branch_items.csv
    ----------------------------
    id            - (series nextval)
    lesson_id  - (FK from lesson.id) <- course_branch_id, course_lesson_id
    branch_ext_id <- course_branch_id
    ext_id        <- course_item_id

    type_id       <- course_item_type_id
    ord           <- course_branch_item_order
    name          <- course_branch_item_name

    optional      <- course_branch_item_optional
    graded        <- is_graded

    load_id       - (current load.id)
    """
    print(tm(), inspect.stack()[0][3], locals())
    with myzip.open('course_branch_items.csv') as from_archive:
        df = pd.read_csv(from_archive, escapechar='\\')
    df = df.sort_values(by=['course_branch_id', 'course_lesson_id'])

    branch_ext_id = ''
    lesson_ext_id = ''
    for index, row in df.iterrows():
        # lesson.id Postgres'а для внешнего ключа:
        if branch_ext_id != row['course_branch_id'] or lesson_ext_id != row['course_lesson_id']:
            branch_ext_id = row['course_branch_id']
            lesson_ext_id = row['course_lesson_id']
            lesson_sql = f"select id from coursera_structure.lesson where load_id={load_id} and\
             branch_ext_id='{branch_ext_id}' and ext_id='{lesson_ext_id}'"
            lesson_id = pd.read_sql_query(lesson_sql, conn)['id'].values[0]

        ext_id = row['course_item_id']
        type_id = row['course_item_type_id']
        ord_ = row['course_branch_item_order']
        name = row['course_branch_item_name'].replace("'", "''")
        optional = row['course_branch_item_optional']
        graded = 'f'  # row['is_graded'] !!! - не во всех версиях загрузок (срез курса) есть это поле

        sql = \
            f"insert into coursera_structure.item (lesson_id, branch_ext_id, ext_id, type_id, ord, name, optional, graded, load_id) \
        values ({lesson_id}, '{branch_ext_id}', '{ext_id}', {type_id}, {ord_}, '{name}', '{optional}', '{graded}', {load_id})"

        cursor.execute(sql)

    return 0


def add_assessment(myzip, load_id, cursor):
    """
    DB.assessment     | assessments.csv
    ----------------------------
    id            - (series nextval)
    ext_id        <- assessment_id

    type_id       <- assessment_type_id
    update_ts     <- assessment_update_ts
    passing_fract <- assessment_passing_fraction

    load_id       - (current load.id)
    """
    print(tm(), inspect.stack()[0][3], locals())
    with myzip.open('assessments.csv') as from_archive:
        df = pd.read_csv(from_archive, parse_dates=['assessment_update_ts'])

    for index, row in df.iterrows():
        ext_id = row['assessment_id']
        type_id = row['assessment_type_id']
        update_ts = convert4null(row['assessment_update_ts'])
        passing_fract = convert4null(row['assessment_passing_fraction'])

        sql = \
            f"insert into coursera_structure.assessment (ext_id, type_id, update_ts, passing_fract, load_id) \
        values ('{ext_id}', {type_id}, '{update_ts}', {passing_fract}, {load_id})".replace("'#@$'", "null"). \
            replace("#@$", "null")

        cursor.execute(sql)

    return 0


def add_question(myzip, load_id, cursor):
    """
    DB.question   | assessment_questions.csv
    ----------------------------
    id            - (series nextval)
    ext_id        <- assessment_question_id

    type_id       <- assessment_question_type_id
    prompt        <- assessment_question_prompt
    update_ts     <- assessment_question_update_ts

    load_id       - (current load.id)
    """
    print(tm(), inspect.stack()[0][3], locals())
    with myzip.open('assessment_questions.csv') as from_archive:
        df = pd.read_csv(from_archive, escapechar='\\', parse_dates=['assessment_question_update_ts'])

    for index, row in df.iterrows():
        ext_id = row['assessment_question_id']
        type_id = row['assessment_question_type_id']
        prompt = row['assessment_question_prompt'].replace("'", "''")
        update_ts = convert4null(row['assessment_question_update_ts'])

        sql = \
            f"insert into coursera_structure.question (ext_id, type_id, prompt, update_ts, load_id) \
        values ('{ext_id}', {type_id}, '{prompt}', '{update_ts}', {load_id})".replace("'#@$'", "null")

        cursor.execute(sql)

    return 0


def add_item_assm(myzip, load_id, conn, cursor):
    """
    DB.item_assessment                       | course_branch_item_assessments.csv
    -----------------------------------------------------------------------------
    item_id        - (FK from item.id)       <- course_branch_id, course_item_id
    assessment_id  - (FK from assessment.id) <- assessment_id
    """
    print(tm(), inspect.stack()[0][3], locals())
    with myzip.open('course_branch_item_assessments.csv') as from_archive:
        df = pd.read_csv(from_archive)

    for index, row in df.iterrows():
        # item.id Postgres'а для внешнего ключа:
        branch_ext_id = row['course_branch_id']
        item_ext_id = row['course_item_id']
        item_sql = f"select id from coursera_structure.item where load_id={load_id} and\
         branch_ext_id='{branch_ext_id}' and ext_id='{item_ext_id}'"
        item_id = pd.read_sql_query(item_sql, conn)['id'].values[0]

        # assessment.id Postgres'а для внешнего ключа:
        assessment_ext_id = row['assessment_id']
        assessment_sql = f"select id from coursera_structure.assessment where load_id={load_id} \
            and ext_id='{assessment_ext_id}'"
        assessment_id = pd.read_sql_query(assessment_sql, conn)['id'].values[0]

        sql = \
            f"insert into coursera_structure.item_assessment (item_id, assessment_id) \
        values ({item_id}, {assessment_id})"

        cursor.execute(sql)

    return 0


def add_assm_question(myzip, load_id, conn, cursor):
    """
    DB.assessment_question  | assessment_assessments_questions.csv
    -----------------------------------------------------------------------------
    assessment_id  - (FK from assessment.id) <- assessment_id
    question_id    - (FK from question.id)   <-  assessment_question_id
    internal_id         <-  assessment_question_internal_id
    cuepoint            <-  assessment_question_cuepoint
    ord                 <-  assessment_question_order
    weight              <-  assessment_question_weight
    """
    print(tm(), inspect.stack()[0][3], locals())
    with myzip.open('assessment_assessments_questions.csv') as from_archive:
        df = pd.read_csv(from_archive)

    for index, row in df.iterrows():
        # assessment.id Postgres'а для внешнего ключа:
        assm_ext_id = row['assessment_id']
        assessment_sql = f"select id from coursera_structure.assessment where load_id={load_id} \
            and ext_id='{assm_ext_id}'"
        assessment_id = pd.read_sql_query(assessment_sql, conn)['id'].values[0]

        # question.id Postgres'а для внешнего ключа:
        question_ext_id = row['assessment_question_id']
        question_sql = f"select id from coursera_structure.question where load_id={load_id} \
            and ext_id='{question_ext_id}'"
        question_id = pd.read_sql_query(question_sql, conn)['id'].values[0]

        internal_id = row['assessment_question_internal_id']
        cuepoint = convert4null(row['assessment_question_cuepoint'])
        ord_ = convert4null(row['assessment_question_order'])
        weight = convert4null(row['assessment_question_weight'])

        sql = \
            f"insert into coursera_structure.assessment_question (\
            assessment_id, question_id, internal_id, cuepoint, ord, weight) \
        values ({assessment_id}, {question_id}, '{internal_id}', {cuepoint}, {ord_}, {weight})".replace("#@$", "null")

        cursor.execute(sql)

    return 0


def add_option(myzip, load_id, conn, cursor):
    """
    DB.q_option       | assessment_options.csv
    ----------------------------
    id            - (series nextval)
    question_id  - (FK from question.id) <- assessment_question_id
    ext_id        <- assessment_option_id

    display       <- assessment_option_display
    feedback      <- assessment_option_feedback
    correct       <- assessment_option_correct
    index      <- assessment_option_index

    load_id       - (current load.id)
    """
    print(tm(), inspect.stack()[0][3], locals())
    with myzip.open('assessment_options.csv') as from_archive:
        df = pd.read_csv(from_archive, escapechar='\\')
    df = df.sort_values(by=['assessment_question_id'])

    question_ext_id = ''
    for index, row in df.iterrows():
        # lesson.id Postgres'а для внешнего ключа:
        if question_ext_id != row['assessment_question_id']:
            question_ext_id = row['assessment_question_id']
            question_sql = f"select id from coursera_structure.question where load_id={load_id} \
            and ext_id='{question_ext_id}'"
            question_id = pd.read_sql_query(question_sql, conn)['id'].values[0]

        str_ext_id = str(row['assessment_option_id'])
        ext_id = str_ext_id[0:10] if str_ext_id[0:2] == '0.' else str_ext_id
        #print('add_option 1', question_ext_id, index, row['assessment_option_display'])
        display = row['assessment_option_display'].replace("'", "''")
        #print('add_option 2', question_ext_id, index, row['assessment_option_feedback'])
        feedback = convert4null(row['assessment_option_feedback']).replace("'", "''")
        correct = row['assessment_option_correct']
        o_index = convert4null(row['assessment_option_index'])

        sql = \
            f"insert into coursera_structure.q_option (\
            question_id, ext_id, display, feedback, correct, index, load_id) \
        values ({question_id}, '{ext_id}', '{display}', '{feedback}', '{correct}', {o_index}, {load_id})".replace("#@$",
                                                                                                                "null")

        cursor.execute(sql)

    return 0


def tm():
    return datetime.datetime.now().isoformat()


def load_structure(folder, zipname, conn):
    """
    Загрузка структуры одного курса из архива (обертка)
    """

    cursor = conn.cursor()

    course_id, load_id = add_load(zipname, cursor)

    print(tm(), inspect.stack()[0][3], locals())

    if not course_id:
        myzip = zipfile.ZipFile(f'{folder}/{zipname}')

        upsert_reference_tables(myzip)

        course_id = add_course(myzip, load_id, conn, cursor)

        add_branch(myzip, load_id, conn, cursor)

        add_module(myzip, load_id, conn, cursor)

        add_lesson(myzip, load_id, conn, cursor)

        add_item(myzip, load_id, conn, cursor)

        add_assessment(myzip, load_id, cursor)

        add_item_assm(myzip, load_id, conn, cursor)

        add_question(myzip, load_id, cursor)

        add_assm_question(myzip, load_id, conn, cursor)

        add_option(myzip, load_id, conn, cursor)

    print(zipname, '\n')

    return course_id, load_id

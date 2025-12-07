import mysql.connector, csv, os 

def get_connection(): 
    mydb = mysql.connector.connect(
    host="localhost",
    user="test",
    password="password",
    database="cs122a"
    )
    return mydb

def format(N): 
    placeholder = []
    for i in range(N - 1):
        placeholder.append('%s,')
    placeholder.append('%s')
    formatted = ''.join(placeholder)
    return formatted

def extract_csv(folder_name, cursor):
    order = ['User', 'AgentCreator', 'AgentClient', 'InternetService', 'LLMService', 'DataStorage', 'BaseModel', 'Configuration', 'CustomizedModel', 'ModelServices', 'ModelConfigurations']
    for table in order:
        with open(f"./{folder_name}/{table}.csv", "r") as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                query = f"INSERT into {table} VALUES ({format(len(row))})"
                cursor.execute(query, row)

def import_data(folder_name):
    db = get_connection()
    cursor = db.cursor()
    try:
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        tables = ['AgentClient', 'AgentCreator', 'BaseModel', 'Configuration', 'CustomizedModel', 
            'DataStorage', 'InternetService', 'LLMService', 'ModelConfigurations', 
            'ModelServices', 'User']

        for table in tables:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")

        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")

        DDL_statements = [
            '''
                CREATE TABLE User (
                uid INT,
                email TEXT NOT NULL,
                username TEXT NOT NULL,
                PRIMARY KEY (uid)
            );
            ''',
            '''
                CREATE TABLE AgentCreator (
                uid INT,
                bio TEXT,
                payout TEXT,
                PRIMARY KEY (uid),
                FOREIGN KEY (uid) REFERENCES User(uid) ON DELETE CASCADE
            );
            ''',
            '''
                CREATE TABLE AgentClient (
                uid INT,
                interests TEXT NOT NULL,
                cardholder TEXT NOT NULL,
                expire DATE NOT NULL,
                cardno BIGINT NOT NULL,
                cvv INT NOT NULL,
                zip INT NOT NULL,
                PRIMARY KEY (uid),
                FOREIGN KEY (uid) REFERENCES User(uid) ON DELETE CASCADE
            );
            ''',
            '''
                CREATE TABLE BaseModel (
                bmid INT,
                creator_uid INT NOT NULL,
                description TEXT NOT NULL,
                PRIMARY KEY (bmid),
                FOREIGN KEY (creator_uid) REFERENCES AgentCreator(uid) ON DELETE CASCADE
            );
            ''',
            '''
            CREATE TABLE CustomizedModel (
            bmid INT,
            mid INT NOT NULL,
            PRIMARY KEY (bmid, mid),
            FOREIGN KEY (bmid) REFERENCES BaseModel(bmid) ON DELETE CASCADE
            );
            ''',
            '''
            CREATE TABLE Configuration (
            cid INT,
            client_uid INT NOT NULL,
            content TEXT NOT NULL,
            labels TEXT NOT NULL,
            PRIMARY KEY (cid),
            FOREIGN KEY (client_uid) REFERENCES AgentClient(uid) ON DELETE CASCADE
            );
            ''',
            '''
            CREATE TABLE InternetService (
            sid INT,
            provider TEXT NOT NULL,
            endpoints TEXT NOT NULL,
            PRIMARY KEY (sid)
            );
            ''',
            '''
            CREATE TABLE LLMService (
            sid INT,
            domain TEXT,
            PRIMARY KEY (sid),
            FOREIGN KEY (sid) REFERENCES InternetService(sid) ON DELETE CASCADE
            );
            ''',
            '''
            CREATE TABLE DataStorage (
            sid INT,
            type TEXT,
            PRIMARY KEY (sid),
            FOREIGN KEY (sid) REFERENCES InternetService(sid) ON DELETE CASCADE
            );
            ''',
            '''
            CREATE TABLE ModelServices (
            bmid INT NOT NULL,
            sid INT NOT NULL,
            version INT NOT NULL,
            PRIMARY KEY (bmid, sid),
            FOREIGN KEY (bmid) REFERENCES BaseModel(bmid) ON DELETE CASCADE,
            FOREIGN KEY (sid) REFERENCES InternetService(sid) ON DELETE CASCADE
            );
            ''',
            '''
            CREATE TABLE ModelConfigurations (
            bmid INT NOT NULL,
            mid INT NOT NULL,
            cid INT NOT NULL,
            duration INT NOT NULL,
            PRIMARY KEY (bmid, mid, cid),
            FOREIGN KEY (bmid, mid) REFERENCES CustomizedModel(bmid, mid) ON DELETE CASCADE,
            FOREIGN KEY (cid) REFERENCES Configuration(cid) ON DELETE CASCADE
            );
            '''
        ]

        for statement in DDL_statements:
            cursor.execute(statement)

        extract_csv(folder_name, cursor)

        db.commit()
        print("Success")
    except Exception as e:
        print(f"Fail: {e}")
    finally:
        cursor.close()
        db.close()

def insert_agent_client(uid, username, email, card_number, card_holder, expiration_date, cvv, zip, interests):
    db = get_connection()
    cursor = db.cursor()
    try:
        user_query = f'''
            INSERT IGNORE into User VALUES ({format(3)})
        '''
        cursor.execute(user_query, [uid, email, username])
        agent_client = f'''
            INSERT into AgentClient VALUES ({format(7)})
        '''
        cursor.execute(agent_client, [uid, interests, card_holder, expiration_date, card_number, cvv, zip])
        
        db.commit()
        print("Success")
    except Exception as e:
        print("Fail") 
    finally:
        cursor.close()
        db.close()

def add_customized_model(mid, bmid):
    db = get_connection()
    cursor = db.cursor()
    try:
        customized_model = f'''
            INSERT into CustomizedModel VALUES ({format(2)})
        '''
        cursor.execute(customized_model, [bmid, mid])

        db.commit()
        print("Success")
    except Exception as e:
        print("Fail")
    finally:
        cursor.close()
        db.close()

def delete_base_model(bmid):
    db = get_connection()
    cursor = db.cursor()
    try:
        base_model = f'''
            DELETE FROM BaseModel WHERE bmid = {bmid}
        '''
        cursor.execute(base_model)

        if cursor.rowcount == 0:
            print("Fail")
        else:
            db.commit()
            print("Success")
    except Exception as e:
        print("Fail") 
    finally:
        cursor.close()
        db.close()
    

def list_internet_service(bmid):
    db = get_connection()
    cursor = db.cursor()

    internet_service = f'''
        SELECT service.sid, service.endpoints, service.provider
        FROM BaseModel model 
        JOIN ModelServices ms ON model.bmid = ms.bmid
        JOIN InternetService service ON service.sid = ms.sid
        WHERE model.bmid = {bmid}
        ORDER BY service.provider ASC
    '''

    cursor.execute(internet_service)
    results = cursor.fetchall()
    
    for row in results:
        print(f"{row[0]},{row[1]},{row[2]}")
    
    cursor.close()
    db.close()

def count_customized_model(bmid_list):
    db = get_connection()
    cursor = db.cursor()
    count_query = f'''
        SELECT customized.bmid, base.description, COUNT(*) 
        FROM CustomizedModel customized
        JOIN BaseModel base ON base.bmid = customized.bmid 
        WHERE customized.bmid IN ({format(len(bmid_list))})
        GROUP BY customized.bmid, base.description
        ORDER BY customized.bmid ASC
    '''

    cursor.execute(count_query, tuple(bmid_list))
    results = cursor.fetchall()
    
    for row in results:
        print(f"{row[0]},{row[1]},{row[2]}")
    
    cursor.close()
    db.close()

def top_N_duration(uid, N):
    db = get_connection()
    cursor = db.cursor()
    duration_query = f'''
        SELECT config.client_uid, model.cid, config.labels, config.content, MAX(model.duration)
        FROM ModelConfigurations model 
        JOIN Configuration config ON config.cid = model.cid 
        WHERE config.client_uid = {uid}
        GROUP BY config.cid, config.labels, config.content
        ORDER BY MAX(model.duration) DESC
        LIMIT {N}
    '''

    cursor.execute(duration_query)
    results = cursor.fetchall()
    
    for row in results:
        print(f"{row[0]},{row[1]},{row[2]},{row[3]},{row[4]}")
    
    cursor.close()
    db.close()

def keyword_search(keyword):
    db = get_connection()
    cursor = db.cursor()
    keyword_query = '''
        SELECT model.bmid, model.sid, service.provider, llm.domain
        FROM ModelServices model
        JOIN BaseModel bm ON bm.bmid = model.bmid
        JOIN InternetService service ON service.sid = model.sid
        JOIN LLMService llm ON llm.sid = model.sid
        WHERE llm.domain LIKE %s
        ORDER BY model.bmid ASC
        LIMIT 5
    '''

    cursor.execute(keyword_query, (f'%{keyword}%',))
    results = cursor.fetchall()
    
    for row in results:
        print(f"{row[0]},{row[1]},{row[2]},{row[3]}")

    cursor.close()
    db.close()


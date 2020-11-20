import configparser
import configparser
import mysql.connector
import logging

def setloglevel(loglevel):
    logger = logging.getLogger()
    fhandler = logging.FileHandler(filename='pipeline.log', mode='a')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fhandler.setFormatter(formatter)
    logger.addHandler(fhandler)
    logger.setLevel(loglevel)

def read_db_config(configfile, section):
    logging.info("Reading db config file to a reusable dictionary")
    config = configparser.ConfigParser()
    config.read(configfile)
    dbdict = { k:v for (k, v) in config.items(section)}
    return dbdict

def get_db_connection(configfile,section,usertype):
    logging.info("Getting the DB connection information")
    connection = None
    dbdict = read_db_config(configfile, section)
    try:
        if usertype == 'root':
            connection = mysql.connector.connect(
                                            user=dbdict[usertype+"user"],
                                            password=dbdict[usertype+"pw"],
                                            host=dbdict['host'],
                                            port=dbdict['port'],
                                            allow_local_infile=True)
        elif usertype == 'query':
            connection = mysql.connector.connect(
                                            user=dbdict[usertype+"user"],
                                            password=dbdict[usertype+"pw"],
                                            host=dbdict['host'],
                                            port=dbdict['port'],
                                            database=dbdict['dbname'])
    except mysql.connector.Error as err:
        logging.error(err)
        logging.error("Error Code:", err.errno)
        logging.error("SQLSTATE", err.sqlstate)
        logging.error("Message", err.msg)

    return (connection, dbdict)

def create_infrastructure(connection, dbdict):
    logging.info("Creating DB Infrastructure - New Database, User and Event Table")
    cursor = connection.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS {}".format(dbdict['dbname']))
    try:
        cursor.execute("DROP USER {}".format(dbdict['queryuser']))
    except Exception as error:
        print("Error while dropping user which we will ignore. This is added for MySQL <= 5.6 versions which do not support IF EXISTS for dropping user", error)
    cursor.execute("CREATE USER '{}' IDENTIFIED BY '{}'".format(dbdict['queryuser'], dbdict['querypw']))
    cursor.execute("GRANT ALL ON {}.* TO {}".format(dbdict['dbname'], dbdict['queryuser']))
    cursor.execute("USE {}".format(dbdict['dbname']))
    cursor.execute("DROP TABLE IF EXISTS event_ticket_system")
    create_table_stmt = \
            "CREATE TABLE `event_ticket_system` (\
            `primary_id` int(11) NOT NULL AUTO_INCREMENT,\
            `ticket_id` int(10) DEFAULT NULL,\
            `trans_date` date DEFAULT NULL,\
            `event_id` int(10) DEFAULT NULL,\
            `event_name` varchar(50) DEFAULT NULL,\
            `event_date` date DEFAULT NULL,\
            `event_type` varchar(10) DEFAULT NULL,\
            `event_city` varchar(20) DEFAULT NULL,\
            `event_addr` varchar(100) DEFAULT NULL,\
            `customer_id` int(10) DEFAULT NULL,\
            `price` decimal(12,2) DEFAULT NULL,\
            `num_tickets` int(10) DEFAULT NULL,\
            PRIMARY KEY (`primary_id`)\
            ) ENGINE=InnoDB AUTO_INCREMENT=151 DEFAULT CHARSET=utf8"
    cursor.execute(create_table_stmt)
    cursor.close()

def load_third_party(connection, dbname, file_path_csv):
    logging.info("Load CSV sales Data to Table usng bulk loader")
    load_sql = "LOAD DATA LOCAL INFILE '{}' INTO TABLE {}.event_ticket_system\
                FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\n' \
                (ticket_id, trans_date,event_id, event_name,event_date, event_type,event_city,event_addr,customer_id, price,num_tickets) ;".format(file_path_csv, dbname)
    logging.info(load_sql)
    try:
        cursor = connection.cursor()
        cursor.execute("use {}".format(dbname))
        cursor.execute(load_sql)
        connection.commit()
    except mysql.connector.Error as err:
        logging.error(err)
        logging.error("Error Code:", err.errno)
        logging.error("SQLSTATE", err.sqlstate)
        logging.error("Message", err.msg)
    cursor.close()

def query_popular_events(connection, dbname):
    logging.info("Query events table")
    sql_statement = "SELECT event_name, SUM(num_tickets) as tickets_sold \
                     FROM {}.event_ticket_system \
                    GROUP BY event_name \
                    ORDER BY tickets_sold DESC LIMIT 3".format(dbname)
    cursor = connection.cursor()
    cursor.execute(sql_statement)
    records = cursor.fetchall()
    print ("Here are the three most popular events based on the tickets sold")
    for record in records:
        print(record[0])
    cursor.close()

def run_it(configfile="db.conf",csvfile="third_party_sales.csv",section="mysql", loglevel=logging.INFO):
    setloglevel(loglevel)
    logging.info("Begin")
    (connection, dbdict) = get_db_connection(configfile, section, 'root')
    logging.info(dbdict)
    create_infrastructure(connection, dbdict)
    load_third_party(connection, dbdict['dbname'], csvfile)
    (connection, dbdict) = get_db_connection(configfile, section, 'query')
    query_popular_events(connection, dbdict['dbname'])
    logging.info("End")
    logging.info("=====================")


if __name__ == '__main__':
    #configfile = "C:\\vjn\\vjnspace\\springboard\\sandbox\\miniprojects\\event_ticket_system_pipeline\\db.conf"
    #csvfile = "C:\\\\vjn\\\\vjnspace\\\\springboard\\\\sandbox\\\\miniprojects\\\\event_ticket_system_pipeline\\\\third_party_sales.csv"
    run_it()
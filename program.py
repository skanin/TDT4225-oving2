import itertools
import os
import sys
import traceback
from datetime import datetime
import time
import functools

from haversine import haversine, Unit
from tabulate import tabulate

from DbConnector import DbConnector


class Program:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
        self.keysToSkip = []
        self.tpsToAdd = {}
        self.acitivityTpsToAdd = []
        self.activitiesToAdd = []

    def setMaxGlobal(self):
        self.cursor.execute("SET GLOBAL max_allowed_packet=1073741824;")
        self.cursor.execute("SET GLOBAL net_write_timeout=180;")
        self.db_connection.commit()
        self.cursor.execute("SHOW VARIABLES WHERE variable_name = 'max_allowed_packet';")
        for x in self.cursor:
            print(x)

        self.cursor.execute("SHOW VARIABLES WHERE variable_name = 'net_write_timeout';")
        for x in self.cursor:
            print(x)

    def createUserTable(self):
        query = """CREATE TABLE IF NOT EXISTS User (
                   id VARCHAR(4) NOT NULL PRIMARY KEY,
                   has_labels BOOL)
                """
        print('Creating table User...')
        self.cursor.execute(query)
    
    def createActivityTable(self):
        query = """CREATE TABLE IF NOT EXISTS Activity (
            id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
            user_id VARCHAR(4),
            transportation_mode VARCHAR(30),
            start_date_time DATETIME,
            end_date_time DATETIME,
            CONSTRAINT Activity_fk1 FOREIGN KEY (user_id) REFERENCES User(id) ON DELETE CASCADE
        )
        """
        print('Creating table Activity...')
        self.cursor.execute(query)

    def createTrackPointTable(self):
        query = """
            CREATE TABLE IF NOT EXISTS TrackPoint (
                id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                activity_id INT NULL,
                lat DOUBLE,
                lon DOUBLE,
                altitude INT,
                date_days DOUBLE,
                date_time DATETIME,
                CONSTRAINT TrackPoint_fk1 FOREIGN KEY (activity_id) REFERENCES Activity(id) ON DELETE CASCADE
            )
        """
        print('Creating table TrackPoint...')
        self.cursor.execute(query)
    
    def cleanDB(self):
        queries = ['DROP TABLE IF EXISTS TrackPoint;', 'DROP TABLE IF EXISTS Activity;', 'DROP TABLE IF EXISTS User;']

        for q in queries:
            self.cursor.execute(q, multi=True)
        
        print('Dropped all tables')
        
        self.createUserTable()
        self.createActivityTable()
        self.createTrackPointTable()
        self.db_connection.commit()
        
        print('Cleaned DB')

    def setup(self):
        self.cleanDB()

    def readIds(self):
        with open('dataset/labeled_ids.txt') as f:
            return f.readlines()
    
    def readTrackPoint(self, tp):
        pass

    def insertIntoUser(self, users):
        query = "INSERT INTO User (id, has_labels) VALUES "
        
        for key, val in users.items():
            query += f'("{key}", {val}), '
        query = query[0: len(query) - 2]
        query += ';'

        self.cursor.execute(query)
        self.db_connection.commit()
        print('Inserted users')

    def insertIntoActivity(self, activities, trackpoints, user):
        tps_to_add = []
        while True:
            activityQuery = f'INSERT INTO Activity(user_id, transportation_mode, start_date_time, end_date_time) VALUES '
            try:
                tp = next(trackpoints)
                # if len(tp) > 2500: print(len(tp))
                for activity in activities:
                    yyyy = tp[0][0:4] # Store year of first trackpoint in file
                    mm = tp[0][4:6] # Store month of first trackpoint in file
                    dd = tp[0][6:8] # Store day of first trackpoint in file
                    hh = tp[0][8:10] # Store hour of first trackpoint in file
                    m = tp[0][10:12] # Store minutes of first trackpoint in file
                    ss = tp[0][12:14]  # Store seconds of first trackpoint in file
                    date_trackpoint = f'{yyyy}/{mm}/{dd} {hh}:{m}:{ss}'
                    
                    if date_trackpoint == activity[0] and activity[1] == tp[-1][-1]:
                        self.keysToSkip.append(tp[0])
                        activityQuery += f'("{user}", "{activity[-1]}", "{activity[0]}", "{activity[1]}"), '
                if activityQuery != 'INSERT INTO Activity(user_id, transportation_mode, start_date_time, end_date_time) VALUES ':
                    activityQuery = activityQuery[0: len(activityQuery) - 2] + ";"
                    self.cursor.execute(activityQuery) # Execute query that inserts activity
                    self.db_connection.commit()  # Commit change
                    
                    insertid = self.cursor.lastrowid
                    for trackpoint in tp:
                        if type(trackpoint) == list:
                            trackpoint.insert(0, self.cursor.lastrowid)
                            self.acitivityTpsToAdd.append(tuple(trackpoint))
            except StopIteration:
                break
            
        
    def insertIntoTrackPoint(self, trackpoints, activity=False, user=None):
        while True:
            try:
                tps = next(trackpoints)
                if not activity and tps[0] not in self.keysToSkip:
                    #print(user)
                    q = f'INSERT INTO Activity(user_id, transportation_mode, start_date_time, end_date_time) VALUES ("{user}", NULL, "{tps[1][-1]}", "{tps[-1][-1]}");'
                    self.cursor.execute(q)
                    self.db_connection.commit()
                    insertid = self.cursor.lastrowid
                    for tp in tps:
                        if type(tp) != str:
                            if insertid in self.tpsToAdd:
                                self.tpsToAdd[insertid].append(tp)
                            else:
                                self.tpsToAdd[insertid] = [tuple(tp)]


            except StopIteration:
                break

    def readLabels(self, path):
        activities = []  # Init empty activity list
        with open(path) as f: # Read file at path
            lines = f.readlines()[1:] # Skip header
            for line in lines: # Loop through lines in file
                l = tuple(map(lambda x: x.strip(), line.split('\t'))) # Get each row as elements in list
                activities.append(l) # Add activity to list
        return activities # Return list

    def readTrackPoints(self, paths, root):
        # print('HEEEY')
        trackpoints = {}  # Init empty trackpoints dict
        # print(path)
        for path in paths:  # Loop through files in the path
            # if '021' in root: print(path)
            with open(root + '/' + path, 'r') as f:
                lines = f.read().splitlines(True)[6:]  # Skip headers
                if len(lines) <= 2500:  # If the file are more than 2500 lines, skip it
                    # print(len(lines))
                    tmp = [path.split('.')[0]] # Init temp list for holding trackpoints 
                    for line in lines:  # Loop through every line in the file
                        if len(line) > 0:
                            l = list(map(lambda x: x.strip(), line.split(','))) # Split the line to get a list of the elements, strip \n and \t
                            l[-2] = (l[-2] + ' ' + l[-1]).replace('-', '/')  # Convert date and time to datetime object
                            del l[2]
                            del l[-1]
                            tmp.append(l)
                    yield tmp

    def insertData(self):
        users = {} # Create user dictionaty
        labeledUsers = tuple(map(lambda x: str(x.strip()), self.readIds())) # Find all users that has label
        activities = {} # Create empty activities dict
        trackpoints = {} # Create empty trackpoints dict
        num = 1 # Just for percentage printing

        for root, dirs, files in os.walk('./dataset/Data'):  # Loop through folders
            if len(dirs) > 0 and len(files) == 0:  # Skip folders where the only folder are 'Trajectory'
                continue
            userid = str(root.split('/')[3]) # Get user id from folder name
            users[userid] = userid in labeledUsers  # Insert user into users dic, with value 'has_labels'

        self.insertIntoUser(users)

        count = 0
        for root, dirs, files in os.walk('./dataset/Data'):
            if len(dirs) > 0 and len(files) == 0:  # Skip folders where the only folder are 'Trajectory'
                continue

            userid = str(root.split('/')[3]) # Get user id from folder name

            if 'labels.txt' in files: # If the user has labeled activities ..
                # activities[userid] = self.readLabels(f'{root}/labels.txt') # .. Read it's labeled activities
                # print(root + '/Trajectory')
                print(f'Labels for user {count}/69 - {round(count/69 * 100, 2)}% done')
                self.insertIntoActivity(self.readLabels(f'{root}/labels.txt'), self.readTrackPoints(os.listdir(root + '/Trajectory'), root + '/Trajectory'), userid)
                count += 1
        
        for root, dirs, files in os.walk('./dataset/Data'):
            if len(dirs) > 0 and len(files) == 0:  # Skip folders where the only folder are 'Trajectory'
                continue

            userid = str(root.split('/')[3])  # Get user id from folder name
            
            if "Trajectory" in root: # If we are in a trajectory folder ...
                print(f'Reading trackpoints for user {userid} - {round(num/182 * 100, 2)}% done')
                self.insertIntoTrackPoint(self.readTrackPoints(files, root), user=userid) # Read trackpoints
                num += 1
        
        query = "INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES (%s, %s, %s, %s, %s, %s)"
        print('Inserting labeled trackpoints...')
        self.cursor.executemany(query, self.acitivityTpsToAdd[:len(self.acitivityTpsToAdd) // 2])
        self.db_connection.commit()
        print('Committed first')
        self.cursor.executemany(query, self.acitivityTpsToAdd[len(self.acitivityTpsToAdd) // 2 :])
        self.db_connection.commit()
        print('Committed second')

        for insertid, tps in self.tpsToAdd.items():
            q = 'INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES '
            for tp in tps:
                q += f'("{insertid}", {tp[0]}, {tp[1]}, {tp[2]}, {tp[3]}, "{tp[4]}"), '
            q = q[: len(q) - 2] + ';'
            self.cursor.execute(q)
        self.db_connection.commit()
    
    def task2point1(self):
        print('################################')
        print('Task 2.1')
        query = "SELECT (SELECT COUNT(*) FROM User as User_count), (SELECT COUNT(*) FROM Activity as Activity_count), (SELECT COUNT(*) FROM TrackPoint as TrackPoint_count);"

        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        print(tabulate(rows, headers=["User count", "Activity count", "Trackpoint count"]))
    
    def task2point2(self):
        print('################################')
        print('Task 2.2')
        query = 'SELECT (SELECT COUNT(*) FROM Activity)/(SELECT COUNT(*) FROM User) AS "Average activity pr user";'

        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        print(tabulate(rows, headers=self.cursor.column_names))

    def task2point3(self):
        print('################################')
        print('Task 2.3')
        query = 'SELECT u.id, COUNT(*) AS num_activities FROM User u JOIN Activity a ON u.id = a.user_id GROUP BY u.id ORDER BY num_activities DESC LIMIT 20;'

        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        print(tabulate(rows, headers=self.cursor.column_names))

    def task2point4(self):
        print('################################')
        print('Task 2.4')
        query = "SELECT DISTINCT u.id AS have_ridden_taxi FROM User u JOIN Activity a ON u.id = a.user_id WHERE a.transportation_mode = 'taxi';"

        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        print(tabulate(rows, headers=self.cursor.column_names))

    def task2point5(self):
        print('################################')
        print('Task 2.5')
        query = 'SELECT transportation_mode, COUNT(*) as num_activities FROM Activity WHERE transportation_mode IN (SELECT DISTINCT transportation_mode FROM Activity) GROUP BY transportation_mode;'

        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        print(tabulate(rows, headers=self.cursor.column_names))

    def task2point6a(self):
        print('################################')
        print('Task 2.6a')
        query = 'SELECT COUNT(*) as ActivityCount, YEAR(start_date_time) as yyyy from Activity GROUP BY YEAR(start_date_time) ORDER BY ActivityCount DESC LIMIT 1;'

        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        print(tabulate(rows, headers=self.cursor.column_names))
        return rows[0][1]
    
    def task2point6b(self):
        print('################################')
        print('Task 2.6b')
        query = 'SELECT YEAR(start_date_time) as yyyy, SUM(HOUR(TIMEDIFF(end_date_time, start_date_time)) + MINUTE(TIMEDIFF(end_date_time, start_date_time))/60 + SECOND(TIMEDIFF(end_date_time, start_date_time))/3600) AS Sum_hours FROM Activity GROUP BY YEAR(start_date_time) ORDER BY Sum_hours DESC LIMIT 1;'

        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        print(tabulate(rows, headers=self.cursor.column_names))

        if self.task2point6a() == rows[0][0]:
            print('Yes, the year is also the year with most recorded hours')
        else:
            print('No, the year is not also the year with most recorded hours')

    def task2point7(self):
        print('################################')
        print('Task 2.7')
        query = 'SELECT TrackPoint.lon, TrackPoint.lat, TrackPoint.activity_id FROM TrackPoint JOIN Activity ON TrackPoint.activity_id = Activity.id WHERE user_id = "112" AND transportation_mode = "walk" AND YEAR(start_date_time) = 2008;'

        self.cursor.execute(query)

        activities = {}
        for activity in self.cursor.fetchall():
            if activity[2] in activities:
                activities[activity[2]].append(activity[:2])
            else:
                activities[activity[2]] = [activity[:2]]

        count = 0
        for key, val in activities.items():
            for i in range(0, len(val) - 1):
                point1 = (val[i][0], val[i][1])
                point2 = (val[i + 1][0], val[i + 1][1])
                
                count += haversine(point1, point2, unit='km')
        
        print(f'User 112 walked {count}km in 2008')


def main():
    # try:
    program = Program()
    # program.cleanDB()
    # program.setMaxGlobal()
    # print('##############')
    # start = time.time()
    # print('Inserting data...')
    # program.insertData()
    # print('Inserted data')
    # print('##############')
    # print(f'{time.time() - start} seconds')
    program.task2point1()
    print()
    program.task2point2()
    print()
    program.task2point3()
    print()
    program.task2point4()
    print()
    program.task2point5()
    print()
    program.task2point6a()
    print()
    program.task2point6b()
    print()
    program.task2point7()
    # except Exception as e:
        # print(e)
        # traceback.print_tb(sys.exc_info()[2])


if __name__ == '__main__':
    main()

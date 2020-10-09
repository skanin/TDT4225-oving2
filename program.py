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
        """Set db max allowed packet size and up the timeout"""
        self.cursor.execute("SET GLOBAL max_allowed_packet=1073741824;")
        self.cursor.execute("SET GLOBAL net_write_timeout=180;")
        self.db_connection.commit()

    def createUserTable(self):
        """Create the users table"""
        query = """CREATE TABLE IF NOT EXISTS User (
                   id VARCHAR(4) NOT NULL PRIMARY KEY,
                   has_labels BOOL)
                """
        print('Creating table User...')
        self.cursor.execute(query)
    
    def createActivityTable(self):
        """Create the Activity table"""
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
        """Create TrackPoint table"""
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
        """Drop all tables and create them again"""
        queries = ['DROP TABLE IF EXISTS TrackPoint;', 'DROP TABLE IF EXISTS Activity;', 'DROP TABLE IF EXISTS User;']

        for q in queries:
            self.cursor.execute(q, multi=True)
        
        print('Dropped all tables')
        
        self.createUserTable()
        self.createActivityTable()
        self.createTrackPointTable()
        self.db_connection.commit()
        
        print('Cleaned DB')

    def readIds(self):
        """Read what users has labels"""
        with open('dataset/labeled_ids.txt') as f:
            return f.readlines()


    def insertIntoUser(self, users):
        """Insert into users table"""
        query = "INSERT INTO User (id, has_labels) VALUES "
        
        for key, val in users.items():
            query += f'("{key}", {val}), ' # Add all users to the sql query
        query = query[0: len(query) - 2] # Finish of the query
        query += ';'

        self.cursor.execute(query) # Execute the query
        self.db_connection.commit() # Commit changes to database
        print('Inserted users')

    def insertIntoActivity(self, activities, trackpoints, user):
        """Inserts into activities table AND finds corresponding trackpoints"""
        tps_to_add = []
        while True:
            activityQuery = f'INSERT INTO Activity(user_id, transportation_mode, start_date_time, end_date_time) VALUES '
            try:
                tp = next(trackpoints) # Get next trackpoint from generator
                
                for activity in activities: # Loop through activities
                    yyyy = tp[0][0:4] # Store year of first trackpoint in file
                    mm = tp[0][4:6] # Store month of first trackpoint in file
                    dd = tp[0][6:8] # Store day of first trackpoint in file
                    hh = tp[0][8:10] # Store hour of first trackpoint in file
                    m = tp[0][10:12] # Store minutes of first trackpoint in file
                    ss = tp[0][12:14]  # Store seconds of first trackpoint in file
                    date_trackpoint = f'{yyyy}/{mm}/{dd} {hh}:{m}:{ss}'
                    
                    if date_trackpoint == activity[0] and activity[1] == tp[-1][-1]: # If we have a full match on activity start and end
                        self.keysToSkip.append(tp[0]) # We skip this trackpoint later on
                        activityQuery += f'("{user}", "{activity[-1]}", "{activity[0]}", "{activity[1]}"), ' # We add activity to query
                if activityQuery != 'INSERT INTO Activity(user_id, transportation_mode, start_date_time, end_date_time) VALUES ':
                    # If we have added activity to query
                    activityQuery = activityQuery[0: len(activityQuery) - 2] + ";" # We finish off the query
                    self.cursor.execute(activityQuery) # Execute query that inserts activity
                    self.db_connection.commit()  # Commit change
                    
                    insertid = self.cursor.lastrowid # Get activity insert id
                    for trackpoint in tp: # Loop through all trackpoints in this activity
                        if type(trackpoint) == list:
                            trackpoint.insert(0, self.cursor.lastrowid)
                            self.acitivityTpsToAdd.append(tuple(trackpoint)) # And add them to 'activityTpsToAdd' for inserting later.
            except StopIteration: # if we do not have more elements in the generator
                break # break
            
        
    def prepareTrackPoints(self, trackpoints, activity=False, user=None):
        """Inserts new acties to Activity table and prepares trackpoints for insert"""
        while True:
            try:
                tps = next(trackpoints) # get next from generator
                if not activity and tps[0] not in self.keysToSkip:  # If we are not to skip this activity and it is not an activity with label
                    q = f'INSERT INTO Activity(user_id, transportation_mode, start_date_time, end_date_time) VALUES ("{user}", NULL, "{tps[1][-1]}", "{tps[-1][-1]}");'
                    # We add a new activity with transportation mode NULL to the query.
                    self.cursor.execute(q) # And execute the query.
                    self.db_connection.commit() # Adn commit the change
                    insertid = self.cursor.lastrowid # We get the last insert id.
                    for tp in tps: # For all trackpoints in the activity
                        if type(tp) != str:
                            if insertid in self.tpsToAdd: 
                                self.tpsToAdd[insertid].append(tp) # Append trackpoint to dict with insert id as key
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
                            l[-2] = (l[-2] + ' ' + l[-1]).replace('-', '/')  # Convert date and time to datetime string
                            del l[2] # Delete third element, we don't need it
                            del l[-1] # Delete last value, because it's merged with l[-2]
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
                self.prepareTrackPoints(self.readTrackPoints(files, root), user=userid) # Read trackpoints
                num += 1
        
        # Add all trackpoints with a transportation mode
        query = "INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES (%s, %s, %s, %s, %s, %s)"
        print('Inserting labeled trackpoints...')
        self.cursor.executemany(query, self.acitivityTpsToAdd[:len(self.acitivityTpsToAdd) // 2])
        self.db_connection.commit()
        self.cursor.executemany(query, self.acitivityTpsToAdd[len(self.acitivityTpsToAdd) // 2 :])
        self.db_connection.commit()

        # Add all trackpoints with transportation mode NULL.
        for insertid, tps in self.tpsToAdd.items():
            q = 'INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES '
            for tp in tps:
                q += f'("{insertid}", {tp[0]}, {tp[1]}, {tp[2]}, {tp[3]}, "{tp[4]}"), '
            q = q[: len(q) - 2] + ';'
            self.cursor.execute(q)
        self.db_connection.commit()
    
    ####################################################
    # Task 2.
    def task2point1(self):
        """Returns how many users, trackpoints and activies that are in the database"""
        print('################################')
        print('Task 2.1')
        query = "SELECT (SELECT COUNT(*) FROM User as User_count), (SELECT COUNT(*) FROM Activity as Activity_count), (SELECT COUNT(*) FROM TrackPoint as TrackPoint_count);"

        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        print(tabulate(rows, headers=["User count", "Activity count", "Trackpoint count"]))
    
    def task2point2(self):
        """Finds average number of activities pr user"""
        print('################################')
        print('Task 2.2')
        query = 'SELECT (SELECT COUNT(*) FROM Activity)/(SELECT COUNT(*) FROM User) AS "Average activity pr user";'

        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        print(tabulate(rows, headers=self.cursor.column_names))

    def task2point3(self):
        """Finds top 20 users with highest number of activities"""
        print('################################')
        print('Task 2.3')
        query = 'SELECT u.id, COUNT(*) AS num_activities FROM User u JOIN Activity a ON u.id = a.user_id GROUP BY u.id ORDER BY num_activities DESC LIMIT 20;'

        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        print(tabulate(rows, headers=self.cursor.column_names))

    def task2point4(self):
        """Finds all users that have ridden taxi"""
        print('################################')
        print('Task 2.4')
        query = "SELECT DISTINCT u.id AS have_ridden_taxi FROM User u JOIN Activity a ON u.id = a.user_id WHERE a.transportation_mode = 'taxi';"

        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        print(tabulate(rows, headers=self.cursor.column_names))

    def task2point5(self):
        """Finds all transportation_mode and their count"""
        print('################################')
        print('Task 2.5')
        query = 'SELECT transportation_mode, COUNT(*) as num_activities FROM Activity WHERE transportation_mode IN (SELECT DISTINCT transportation_mode FROM Activity) GROUP BY transportation_mode;'

        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        print(tabulate(rows, headers=self.cursor.column_names))

    def task2point6a(self, inTaskB=False):
        """Finds year with most activities"""
        if not inTaskB:
            print('################################')
            print('Task 2.6a')
        query = 'SELECT COUNT(*) as ActivityCount, YEAR(start_date_time) as yyyy from Activity GROUP BY YEAR(start_date_time) ORDER BY ActivityCount DESC LIMIT 1;'

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        if not inTaskB:
            print(tabulate(rows, headers=self.cursor.column_names))
        return rows[0][1]
    
    def task2point6b(self):
        """Finds if the year with most activities is also most recorded hours"""
        print('################################')
        print('Task 2.6b')
        query = 'SELECT YEAR(start_date_time) as yyyy, SUM(HOUR(TIMEDIFF(end_date_time, start_date_time)) + MINUTE(TIMEDIFF(end_date_time, start_date_time))/60 + SECOND(TIMEDIFF(end_date_time, start_date_time))/3600) AS Sum_hours FROM Activity GROUP BY YEAR(start_date_time) ORDER BY Sum_hours DESC LIMIT 1;'

        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        print(tabulate(rows, headers=self.cursor.column_names))

        if self.task2point6a(True) == rows[0][0]:
            print('Yes, the year is also the year with most recorded hours')
        else:
            print('No, the year is not also the year with most recorded hours')

    def task2point7(self):
        """Finds total distance walked in 2008 by user 112"""
        print('################################')
        print('Task 2.7')
        query = """SELECT t.lat, t.lon, t.activity_id FROM User u JOIN Activity a ON u.id = a.user_id JOIN TrackPoint t ON a.id = t.activity_id WHERE u.id = '112' AND transportation_mode = 'walk' AND YEAR(start_date_time) = '2008' AND YEAR(end_date_time) = '2008';"""
        
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
    
    def task2point8(self):
        """
        Finds top 20 users who have gained most altitude
        """
        print('################################')
        print('Task 2.8')
        # query = 'SELECT Activity.user_id, altitude, Activity.id FROM Activity JOIN TrackPoint ON Activity.id = TrackPoint.activity_id WHERE altitude != -777 ORDER BY TrackPoint.id ASC;'

        # self.cursor.execute(query)

        # activities = {}
        # for activity in self.cursor.fetchall():
        #     if activity[2] in activities:
        #         activities[activity[2]].append(activity[:2])
        #     else:
        #         activities[activity[2]] = [activity[:2]]

        # altitudeGained = {}
        # users = {}
        # for key, val in activities.items():
        #     altitude = 0
        #     user = val[0][0]
        #     for i in range(1, len(val)-1):
        #         last_altitude = val[i-1][1]
        #         if val[i][1] > last_altitude:
        #             altitude += val[i][1] - last_altitude
        #     if user in users:
        #         users[user].append(altitude)
        #     else:
        #         users[user] = [altitude]

        # all_users = []

        # for key in users:
        #     all_users.append((key, sum(users[key])*0.3048))
        
        # all_users = sorted(all_users, key=lambda x: x[1], reverse=True)[0:20]

        # print(tabulate(all_users, headers=['id', 'total meters gained pr user']))
        query = """
            SELECT sub.UserID AS "ID", sub.Altitude_m AS "Total meters gained"
            FROM ( 
                SELECT 
                    Activity.user_id AS userID, 
                    SUM(CASE WHEN tp1.altitude IS NOT NULL AND
                    tp2.altitude IS NOT NULL 
                    THEN (tp2.altitude - tp1.altitude) * 0.3048000 ELSE 0 END) AS Altitude_m 
                FROM 
                    TrackPoint AS tp1 JOIN TrackPoint AS tp2 ON tp1.activity_id=tp2.activity_id AND 
                    tp1.id+1 = tp2.id JOIN Activity ON Activity.id = tp1.activity_id AND Activity.id = tp2.activity_id 
                WHERE tp2.altitude > tp1.altitude 
                GROUP BY Activity.user_id ) AS sub 
            ORDER BY Altitude_m DESC 
            LIMIT 20;
        """

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))


    def task2point9(self):
        """Finds all users who have invalid activities and count of invalid activities"""
        print('################################')
        print('Task 2.9')
        # query = 'SELECT a.id, a.user_id, tp.date_days FROM TrackPoint as tp JOIN Activity as a ON tp.activity_id = a.id;'

        # self.cursor.execute(query)

        # activities = {}
        # for activity in self.cursor.fetchall():
        #     if activity[0] in activities:
        #         activities[activity[0]].append(activity[1:])
        #     else:
        #         activities[activity[0]] = [activity[1:]]
        

        # users_with_invalid_activities = {}
        # for key, val in activities.items():
        #     user = val[0][0]
        #     for i in range(0, len(val) - 1):
        #         if val[i + 1][1] - val[i][1] >= 0.00347222222:
        #             # print(key)
        #             if user in users_with_invalid_activities:
        #                 users_with_invalid_activities[user] += 1
        #             else:
        #                 users_with_invalid_activities[user] = 1
        #             break
        
        # print(users_with_invalid_activities)
        # all_users = []
        # for key in users_with_invalid_activities:
        #     all_users.append((key, users_with_invalid_activities[key]))
        
        # print(tabulate(all_users, headers=["User", "Number of invalid activities"]))

        query = """
                SELECT 
                        Activity.user_id AS "User", COUNT(DISTINCT(activity_id)) as "Number of illegal activities"
                FROM (
                    SELECT
                        TP1.activity_id AS activity_id, (TP2.date_days - TP1.date_days) AS diff
                    FROM 
                        TrackPoint AS TP1 INNER JOIN TrackPoint AS TP2 ON TP1.activity_id=TP2.activity_id AND TP1.id+1=TP2.id
                    HAVING 
                        diff >= 0.00347222222
                    ) AS sub JOIN Activity ON Activity.id = sub.activity_id

                GROUP BY Activity.user_id
                ORDER BY COUNT(DISTINCT(activity_id)) ASC;
                """
        self.cursor.execute(query)

        rows = self.cursor.fetchall()

        print(tabulate(rows, headers=self.cursor.column_names))

    def task2point10(self):
        """Finds the users that have tracked activities in the Forbidden city of Beijing"""
        print('################################')
        print('Task 2.10')

        # query = 'SELECT a.user_id, tp.lat, tp.lon FROM Activity as a JOIN TrackPoint as tp ON a.id = tp.activity_id;'

        # self.cursor.execute(query)
        
        # users_in_forbidden_city = []
        
        # for activity in map(lambda x: [x[0], round(x[1], 3), round(x[2], 3)], self.cursor.fetchall()):
        #     if [activity[0]] in users_in_forbidden_city:
        #         continue
        #     if activity[1] == 39.916 and activity[2] == 116.397:
        #         users_in_forbidden_city.append([activity[0]])

        # print(tabulate(users_in_forbidden_city, headers=["User in Forbidden City"]))

        query = 'SELECT DISTINCT a.user_id AS "User in Forbidden City" FROM Activity as a JOIN TrackPoint as tp ON a.id = tp.activity_id WHERE ROUND(tp.lon, 3) = 116.397 AND ROUND(tp.lat, 3) = 39.916;'
        self.cursor.execute(query)

        print(tabulate(self.cursor.fetchall(), headers=self.cursor.column_names))

    
    def task2point11(self):
        """Finds all users who have registered transportation_mode and their most used transportation_mode"""
        print('################################')
        print('Task 2.11')
        # query = 'SELECT user_id, transportation_mode  FROM Activity WHERE transportation_mode IN (SELECT DISTINCT transportation_mode FROM Activity);'

        # self.cursor.execute(query)

        # users = {}
        # for row in self.cursor.fetchall():
        #     user = row[0]
        #     mode = row[1]
        #     if user in users:
        #         if mode in users[user]:
        #             users[user][mode] += 1
        #         else:
        #             users[user][mode] = 1
        #     else:
        #         users[user] = {mode: 1}

        # all_users = []
        # for user, modes in users.items():
        #     trans_count = 0
        #     trans_mode = None
        #     for mode, count in modes.items():
        #         if count > trans_count:
        #             trans_count = count
        #             trans_mode = mode
        #     all_users.append([user, trans_mode])
        
        # print(tabulate(all_users, headers=["user_id", "most_used_transportation_mode"]))

        query = """
        SELECT sub2.user_id AS "User", sub2.transportation_mode AS "Most used transportation mode", sub2.antall
	    FROM (
		    SELECT
			    user_id, transportation_mode, COUNT(transportation_mode) AS antall 
		    FROM 
			    Activity a WHERE transportation_mode IS NOT NULL
		    GROUP BY transportation_mode, user_id
            ) as sub 
            JOIN (
		        SELECT
			        user_id, transportation_mode, COUNT(transportation_mode) AS antall 
		        FROM 
			        Activity a WHERE transportation_mode IS NOT NULL
		        GROUP BY transportation_mode, user_id 
                ) as sub2 ON sub.user_id = sub2.user_id	
        GROUP BY sub2.user_id, sub2.transportation_mode
        HAVING antall = MAX(sub.antall)
        ORDER BY sub2.user_id;
        """
        self.cursor.execute(query)
        rows = list(map(lambda x: x[0:2], self.cursor.fetchall()))

        users = []
        trans_modes = []

        for user in rows:
            if user[0] in users:
                continue
            else:
                users.append(user[0])
                trans_modes.append(user)

        print(tabulate(trans_modes, headers=self.cursor.column_names))

def main():
    # try:
    program = Program()
    program.cleanDB()
    program.setMaxGlobal()
    print('##############')
    start = time.time()
    print('Inserting data...')
    program.insertData()
    print('Inserted data')
    print('##############')
    print(f'{time.time() - start} seconds')
    print('\n'*3)
    print('##################Tasks##################')
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
    print()
    program.task2point8()
    print()
    program.task2point9()
    print()
    program.task2point10()
    print()
    program.task2point11()
    # except Exception as e:
        # print(e)
        # traceback.print_tb(sys.exc_info()[2])


if __name__ == '__main__':
    main()

from DbConnector import DbConnector
from tabulate import tabulate
from datetime import datetime
import os

class Program:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
    
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

    def insertIntoActivity(self, activities):
        for user, tps in activities.items():
            activityQuery = f'INSERT INTO Activity(user_id, transportation_mode, start_date_time, end_date_time) VALUES ("{user}", "{tps[0]}", "{tps[1][-1].strftime("%Y-%m-%d %H:%M:%S")}", "{tps[-1][-1].strftime("%Y-%m-%d %H:%M:%S")}")'
            self.cursor.execute(activityQuery)
            self.db_connection.commit()
            insertId = self.cursor.lastrowid
            query = "INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES "
            for tp in tps:
                if type(tp) != str:
                    print(tp)
                    query += f'({insertId}, {tp[0]}, {tp[1]}, {tp[2]}, {tp[4]}, "{tp[5].strftime("%Y-%m-%d %H:%M:%S")}"), '

            query = query[0: len(query) - 2] + ";"

            self.cursor.execute(query)
            self.db_connection.commit()
            print('Inserted activities and corresponding trackpoints')
        
    def insertIntoTrackPoint(self, trackpoints, labeledTrackpoints):
        query = "INSERT INTO TrackPoint (lat, lon, altitude, date_days, date_time) VALUES "
        for user, trackpoint in trackpoints.items():
            for tpId, tps in trackpoint.items():
                if user in labeledTrackpoints.keys():
                    trackpoints[user][tpId] = list(filter(lambda x: x not in labeledTrackpoints[user][1:], trackpoints[user][tpId]))

                for tp in tps:
                    if len(trackpoints[user][tpId]) > 0:
                        query += f'({tp[0]}, {tp[1]}, {tp[2]}, {tp[4]}, "{tp[5].strftime("%Y-%m-%d %H:%M:%S")}"), '
        
        query = query[0: len(query) - 2] + ";"
        self.cursor.execute(query)
        self.db_connection.commit()
        print("Inserted trackpoints")

    def readLabels(self, path):
        activities = []
        with open(path) as f:
            lines = f.readlines()[1:]
            for line in lines:
                l = list(map(lambda x: x.strip(), line.split('\t')))
                l[0] = datetime.strptime(l[0].replace('/', '-'), '%Y-%m-%d %H:%M:%S')
                l[1] = datetime.strptime(l[1].replace('/', '-'), '%Y-%m-%d %H:%M:%S')
                activities.append(l)
        return activities

    def readTrackPoints(self, paths, root):
        trackpoints = {}
        count = 0
        for path in paths:
            with open(root + '/' + path) as f:
                lines = f.readlines()[6:]
                if len(lines) <= 2500:
                    tmp = []
                    for line in lines:
                        l = list(map(lambda x: x.strip(), line.split(',')))
                        l[-1] = datetime.strptime(l[-2] + ' ' + l[-1], '%Y-%m-%d %H:%M:%S')
                        del l[-2]
                        tmp.append(l)
                    trackpoints[path.split('.')[0]] = tmp
        return trackpoints

    def insertData(self):
        users = {} # Create user dictionaty
        labeledUsers = list(map(lambda x: str(x.strip()), self.readIds())) # Find all users that has label
        activities = {}
        trackpoints = {}
        num = 1
        for root, dirs, files in os.walk('./dataset/Data'): # Loop through folders
            if len(dirs) > 0 and len(files) == 0:  # Skip folders where the only folder are 'Trajectory'
                continue
            userid = str(root.split('/')[3]) # Get user id from folder name
            users[userid] = userid in labeledUsers  # Insert user into users dic, with value 'has_labels'

            if 'labels.txt' in files:
                activities[userid] = self.readLabels(f'{root}/labels.txt')
            
            if "Trajectory" in root: # and '021' in root:
                print(f'Reading trackpoints for user {userid} - {round(num/182 * 100, 2)}% done')
                trackpoints[userid] = self.readTrackPoints(files, root)
                num += 1

        print()
        print('##################################')
        print()
        
        userCount = 1
        labeledTrackpoints = {}
        for user in labeledUsers:
            print(f'Checking for labeles activities for user {userCount}/{len(labeledUsers)} - {round(userCount/len(labeledUsers) * 100, 2)}% done')
            for activity in activities[user]:
                for key, value in trackpoints[user].items():
                    yyyy = key[0:4]
                    mm = key[4:6]
                    dd = key[6:8]
                    hh = key[8:10]
                    m = key[10:12]
                    ss = key[12:14]
                    date_trackpoint = datetime(int(yyyy), int(mm), int(dd), int(hh), int(m), int(ss))
                    if activity[0] == date_trackpoint and value[-1][-1] == activity[1]:
                        if user in labeledTrackpoints:
                            labeledTrackpoints[user].append([activity[-1]] + value)
                        else:
                            labeledTrackpoints[user] = [activity[-1]] + value
            userCount += 1
        
        print()
        print('##################################')
        print()
        
        print("Inserting into users")
        self.insertIntoUser(users)

        print()
        print('##################################')
        print()

        print("Inserting into activity, with corresponding trackpoints")
        self.insertIntoActivity(labeledTrackpoints)

        print()
        print('##################################')
        print()
        
        print("Inserting into trackpoint")
        self.insertIntoTrackPoint(trackpoints, labeledTrackpoints)


def main():
    try:
        program = Program()
        program.cleanDB()
        program.insertData()
    except Exception as e:
        print(e)


if __name__ == '__main__':
    main()
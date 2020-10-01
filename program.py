import itertools
import os
import sys
import traceback
from datetime import datetime
import time

from tabulate import tabulate

from DbConnector import DbConnector


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

    def insertIntoActivity(self, activities, trackpoints):
        userCount = max(map(lambda x: int(x), activities)) # Just for printing percentage
        count = 1 # Just for printing percentage
        for user, tps in activities.items(): # Loop through each user and the corresponding activities
            query = "INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES "
            print(f'Inserting info activities for user {user} - {round(count/userCount * 100, 2)} ')
            for activity in tps: # Loop through each activity in all activities
                activityQuery = f'INSERT INTO Activity(user_id, transportation_mode, start_date_time, end_date_time) VALUES '
                activityQuery += f'("{user}", "{activity[0]}", "{activity[1][-1].strftime("%Y-%m-%d %H:%M:%S")}", "{activity[-1][-1].strftime("%Y-%m-%d %H:%M:%S")}")'
                print(activityQuery)
                self.cursor.execute(activityQuery) # Execute query that inserts activity
                self.db_connection.commit() # Commit change
                insertId = self.cursor.lastrowid # Get last insertid
                for tp in activity[1:]: # Insert the trackpoint that match the activity
                    query += f'({insertId}, {tp[0]}, {tp[1]}, {tp[3]}, {tp[4]}, "{tp[5].strftime("%Y-%m-%d %H:%M:%S")}"), '

            query = query[0: len(query) - 2] + ";"  # Complete query
            self.cursor.execute(query) # Execute the query
            self.db_connection.commit() # Commit changes
            count += 1
        print('Inserted activities and corresponding trackpoints')
        
    def insertIntoTrackPoint(self, trackpoints, labeledTrackpoints):
        userCount = max(map(lambda x: int(x), trackpoints.keys())) # For printing percentage
        count = 1 # For printing percentage
        for user, trackpoint in trackpoints.items(): # Loop through each user and the corresponding trackpoints
            print(f'Insert into trackpoints for user {user} - {round(count/userCount * 100, 2)}')
            query = "INSERT INTO TrackPoint (lat, lon, altitude, date_days, date_time) VALUES "
            for tpId, tps in trackpoint.items(): # Loop through each key in dict and it's trackpoint (each key are one file)
                for tp in tps: # Loop through all trackpoints in the list
                    if len(trackpoints[user][tpId]) > 0 and type(trackpoints[user][tpId][0]) != str: # If the list is not empty 
                        query += f'({tp[0]}, {tp[1]}, {tp[2]}, {tp[4]}, "{tp[5].strftime("%Y-%m-%d %H:%M:%S")}"), ' # Add trackpoint to query
        
            query = query[0: len(query) - 2] + ";" # Complete query
            if query != "INSERT INTO TrackPoint (lat, lon, altitude, date_days, date_time) VALUE;": # If a user did not have any trackpoints, the query would have been default
                self.cursor.execute(query) # execute the query.
            count += 1
        self.db_connection.commit() # Commit the changes.
        print("Inserted trackpoints")

    def readLabels(self, path):
        activities = []  # Init empty activity list
        with open(path) as f: # Read file at path
            lines = f.readlines()[1:] # Skip header
            for line in lines: # Loop through lines in file
                l = list(map(lambda x: x.strip(), line.split('\t'))) # Get each row as elements in list
                l[0] = datetime.strptime(l[0].replace('/', '-'), '%Y-%m-%d %H:%M:%S') # Convert date to datetime object
                l[1] = datetime.strptime(l[1].replace('/', '-'), '%Y-%m-%d %H:%M:%S') # Convert date to datetime object
                activities.append(l) # Add activity to list
        return activities # Return list

    def readTrackPoints(self, paths, root):
        trackpoints = {}  # Init empty trackpoints dict
        
        for path in paths: # Loop through files in the path
            with open(root + '/' + path) as f: # Open each file
                lines = f.readlines()[6:] # Skip headers 
                if len(lines) <= 2500: # If the file are less than or equal to 2500 lines, skip it
                    tmp = [] # Init temp list for holding trackpoints 
                    for line in lines: # Loop through every line in the file
                        l = list(map(lambda x: x.strip(), line.split(','))) # Split the line to get a list of the elements, strip \n and \t
                        l[-1] = datetime.strptime(l[-2] + ' ' + l[-1], '%Y-%m-%d %H:%M:%S') # Convert date and time to datetime object
                        del l[-2] # Delete date, since it's merged with time
                        tmp.append(l) # Append trackpoint to temp list
                    trackpoints[path.split('.')[0]] = tmp # Add list to trackpoints dict, with filename as key
        return trackpoints # Return trackpoint dict

    def insertData(self):
        users = {} # Create user dictionaty
        labeledUsers = list(map(lambda x: str(x.strip()), self.readIds())) # Find all users that has label
        activities = {} # Create empty activities dict
        trackpoints = {} # Create empty trackpoints dict
        num = 1 # Just for percentage printing

        for root, dirs, files in os.walk('./dataset/Data'): # Loop through folders
            if len(dirs) > 0 and len(files) == 0:  # Skip folders where the only folder are 'Trajectory'
                continue
            
            userid = str(root.split('/')[3]) # Get user id from folder name
            users[userid] = userid in labeledUsers  # Insert user into users dic, with value 'has_labels'

            if 'labels.txt' in files: # If the user has labeled activities ..
                activities[userid] = self.readLabels(f'{root}/labels.txt') # .. Read it's labeled activities
            
            if "Trajectory" in root: # If we are in a trajectory folder ...
                print(f'Reading trackpoints for user {userid} - {round(num/182 * 100, 2)}% done')
                trackpoints[userid] = self.readTrackPoints(files, root) # Read trackpoints
                num += 1

        print()
        print('##################################')
        print()

        userCount = 1 # Printing percentage purposes
        labeledTrackpoints = {} # Init empty labeledTrackpoints dict
        for user in labeledUsers:  # Loop through all labeled users
            # print(user)
            # print(f'Checking for labeles activities for user {userCount}/{len(labeledUsers)} - {round(userCount/len(labeledUsers) * 100, 2)}% done')
            for activity in activities[user]: # Loop through users activities
                keys = [] # init empty keys list 
                for key, value in trackpoints[user].items(): # Loop thorugh key (filename of trackpoint) and trackpoints
                    yyyy = key[0:4] # Store year of first trackpoint in file
                    mm = key[4:6] # Store month of first trackpoint in file
                    dd = key[6:8] # Store day of first trackpoint in file
                    hh = key[8:10] # Store hour of first trackpoint in file
                    m = key[10:12] # Store minutes of first trackpoint in file
                    ss = key[12:14] # Store seconds of first trackpoint in file
                    date_trackpoint = datetime(int(yyyy), int(mm), int(dd), int(hh), int(m), int(ss)) # Create datetime object from info above
                    if activity[0] == date_trackpoint and value[-1][-1] == activity[1]: # If the whole trackpoint file match the date and times in activity (last and first datetime)
                        tmp = []
                        # if user == '020':
                            # print(activity)
                        '''
                        legalActivities = ['bike', 'walk', 'bus', 'car', 'train', 'subway', 'airplane', 'boat', 'run', 'motorcycle']
                        if value[0] in legalActivities: # There was one user that had both two activities for same trackpoint, so we have to check for this 
                            tmp = value.copy() # Copy value 
                            tmp[0] = activity[0]
                            print(f'Value 0: {value[0]}, Value 1: {value[1]}')
                        else:
                        '''
                        value.insert(0, activity[-1])

                        if user in labeledTrackpoints.keys():
                            labeledTrackpoints[user].append(tmp if len(tmp) > 0 else value)
                        else:
                            labeledTrackpoints[user] = [tmp if len(tmp) > 0 else value]
                        keys.append(key)
                for key in keys:
                    del trackpoints[user][key]
            userCount += 1

        
        for user, tps in labeledTrackpoints.items():
            for activity in tps:
                if 'bike' in activity and 'walk' in activity:
                    print(activity)
        
        print()
        print('##################################')
        print()
        
        print("Inserting into users")
        self.insertIntoUser(users)

        print()
        print('##################################')
        print()

        print("Inserting into activity, with corresponding trackpoints")
        self.insertIntoActivity(labeledTrackpoints, trackpoints)

        print()
        print('##################################')
        print()
        
        print("Inserting into trackpoint")
        self.insertIntoTrackPoint(trackpoints, labeledTrackpoints)

def main():
    try:
        program = Program()
        program.cleanDB()
        start = time.time()
        program.insertData()
        print('##############')
        print(f'{time.time() - start} seconds')
    except Exception as e:
        print(e)
        # traceback.print_tb(sys.exc_info()[2])


if __name__ == '__main__':
    main()

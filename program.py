from DbConnector import DbConnector
from tabulate import tabulate
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
                activity_id INT NOT NULL,
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
        for key, val in users.items():
            self.cursor.execute(f'INSERT INTO User(id, has_labels) VALUES("{key}", {val})')
        self.db_connection.commit()
        print('Inserted users')

    def readLabels(self, path):
        activities = []
        with open(path) as f:
            lines = f.readlines()[1:]
            for line in lines:
                activities.append(list(map(lambda x: x.strip(), line.split('\t'))))
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
                        tmp.append(list(map(lambda x: x.strip(), line.split('\t'))))
                    trackpoints[path.split('.')[0]] = tmp
                else:
                    if '000' in root:
                        count += 1
        if '000' in root:
            print(count)
        return trackpoints

    def testOsWalk(self):
        users = {} # Create user dictionaty
        labeledUsers = list(map(lambda x: str(x.strip()), self.readIds()))  # Find all users that has label
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
            
            if "Trajectory" in root:
                print(f'Reading trackpoints for user {userid} - Percent done: {round(num/182 * 100, 2)}')
                trackpoints[userid] = self.readTrackPoints(files, root)
                num += 1
        #print(activities['010'])
        print(len(trackpoints['000'].keys()))
        # self.insertIntoUser(users) # Insert users into DB.


def main():
    try:
        program = Program()
        # program.cleanDB()
        program.testOsWalk()
    except Exception as e:
        print(e)


if __name__ == '__main__':
    main()
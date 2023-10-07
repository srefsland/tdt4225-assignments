from DbConnector import DbConnector
from tabulate import tabulate
import os

class Part1:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
    
    def reset_database(self):
        """
        Resets the database by dropping all tables
        """
        query = "DROP TABLE IF EXISTS track_point"
        self.cursor.execute(query)
        query = "DROP TABLE IF EXISTS activity"
        self.cursor.execute(query)
        query = "DROP TABLE IF EXISTS user"
        self.cursor.execute(query)
        self.db_connection.commit()

    def show_tables(self):
        """
        Shows all tables in the database
        """
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))
        
    def create_table_user(self):
        """
        Creates the user table if it does not exist
        """
        query = """
                CREATE TABLE IF NOT EXISTS user (
                    id VARCHAR(50) NOT NULL PRIMARY KEY,
                    has_labels TINYINT(1) NOT NULL
                );
                """
        self.cursor.execute(query)
        self.db_connection.commit()
        
    def create_table_activity(self):
        """
        Creates the activity table if it does not exist
        """
        query = """
                CREATE TABLE IF NOT EXISTS activity (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    user_id VARCHAR(50) NOT NULL,
                    transportation_mode VARCHAR(100),
                    start_date_time DATETIME NOT NULL,
                    end_date_time DATETIME NOT NULL,
                    FOREIGN KEY (user_id) REFERENCES user(id) ON DELETE CASCADE
                );
                """       
        self.cursor.execute(query)
        self.db_connection.commit()
        
    def create_table_track_point(self):
        """
        Creates the track_point table if it does not exist
        """
        query = """
                CREATE TABLE IF NOT EXISTS track_point (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    activity_id INT NOT NULL,
                    lat DOUBLE NOT NULL,
                    lon DOUBLE NOT NULL,
                    altitude INT NOT NULL,
                    date_days DOUBLE NOT NULL,
                    date_time DATETIME NOT NULL,
                    FOREIGN KEY (activity_id) REFERENCES activity(id) ON DELETE CASCADE
                );
                """
        self.cursor.execute(query)
        self.db_connection.commit()
        
        
    def insert_gps_data(self):
        """
        Inserts the GPS data into the database
        """
        user_ids = os.listdir("./dataset/Data")
        
        with open("./dataset/labeled_ids.txt", "r") as f:
            user_labels = f.read().splitlines()
            
        insert_user_query = """
        INSERT INTO user
        (id, has_labels)
        VALUES (%(id)s, %(has_labels)s)
        """
        
        insert_activity_query = """
        INSERT INTO activity
        (user_id, transportation_mode, start_date_time, end_date_time)
        VALUES (%(user_id)s, %(transportation_mode)s, %(start_date_time)s, %(end_date_time)s)
        """
        
        insert_track_point_query = """
        INSERT INTO track_point
        (activity_id, lat, lon, altitude, date_days, date_time)
        VALUES (%(activity_id)s, %(lat)s, %(lon)s, %(altitude)s, %(date_days)s, %(date_time)s)
        """
    
        for user_id in user_ids:
            print(f"Processing user {user_id}")
            user_root = f"./dataset/Data/{user_id}"
            has_label = 1 if user_id in user_labels else 0
            
            self.cursor.execute(insert_user_query, {"id": user_id, "has_labels": has_label})

            for file in os.listdir(f"{user_root}/Trajectory"):
                file_path = f"{user_root}/Trajectory/{file}"
                
                # Get trackpoints if length is sufficiently short
                track_points = self._process_trajectory_file(file_path)
                
                if track_points:
                    start_date_time = f"{track_points[0][5]} {track_points[0][6]}"
                    end_date_time = f"{track_points[-1][5]} {track_points[-1][6]}"
                    
                    if has_label:
                        labels_file_path = f"{user_root}/labels.txt"
                        labels = self._process_labels_file(labels_file_path)
                        
                        transportation_mode = self._get_transportation_mode(start_date_time, end_date_time, labels)
                    else:
                        transportation_mode = None
                    
                    self.cursor.execute(insert_activity_query, 
                                        {"user_id": user_id,
                                         "transportation_mode": transportation_mode,
                                         "start_date_time": start_date_time,
                                         "end_date_time": end_date_time})
                    
                    activity_id = self.cursor.lastrowid
                    
                    # Convert track points to structured list of dictionaries
                    track_points_structured = [{"activity_id": activity_id,
                                                "lat": track_point[0],
                                                "lon": track_point[1],
                                                "altitude": track_point[3],
                                                "date_days": track_point[4],
                                                "date_time": f"{track_point[5]} {track_point[6]}"}
                                                for track_point in track_points]
                    
                    self.cursor.executemany(insert_track_point_query, track_points_structured)  
                    self.db_connection.commit()

    def _get_transportation_mode(self, start_date_time, end_date_time, labels):
        """Fetches the transportation mode for an activity if it exists

        Args:
            start_date_time (string): The start date time of the activity
            labels (list[string]): Raw labels from the labels.txt file

        Returns:
            string: transportation mode if it exists, else None
        """
        for label in labels:
            # Replace / with - in date
            label_start_date_time = f"{label[0].replace('/', '-')} {label[1].replace('/', ':')}"
            label_end_date_time = f"{label[2].replace('/', '-')} {label[3].replace('/', ':')}"            
            # If the start date time of the label matches the start date time of the activity
            # then we have found a labeled transportation mode for this activity
            if label_start_date_time == start_date_time and label_end_date_time == end_date_time:
                transportation_mode = label[4]
                return transportation_mode
            
        return None
                
    def _process_trajectory_file(self, file_path):
        """Processes the plt file and returns the track points if the file is valid

        Args:
            file_path (string): path to the plt file

        Returns:
            list[string]: the track points if the file is valid, else None
        """
        with open(file_path, "r") as f:
            # We only want to read .plt files that contain at most 2506 lines (2500 + 6 header lines)
            num_lines = sum(1 for _ in f)
            if num_lines > 2506:
                print(f"File {file_path} has more than 2500 track points: skipping! {num_lines}")
                return None
            else:
                f.seek(0)
                # Skip first 6 header lines
                track_points_lines = f.read().splitlines()[6:]
                track_points = [line.strip().split(",") for line in track_points_lines]
                
                return track_points
            
    def _process_labels_file(self, file_path):
        """Processes the labels.txt file and returns the labels

        Args:
            file_path (string): the path to the labels.txt file

        Returns:
            list[string]: the raw labels
        """
        with open(file_path, "r") as f:
            labels_lines = f.read().splitlines()[1:]
            labels = [line.split() for line in labels_lines]
            
            return labels
        
    def show_top_10_tables(self):
        user_query = """
        SELECT * FROM user
        LIMIT 10;
        """
        activity_query = """
        SELECT * FROM activity
        LIMIT 10;
        """
        track_point_query = """
        SELECT * FROM track_point
        LIMIT 10;
        """
        self.cursor.execute(user_query)
        rows = self.cursor.fetchall()
        print("User table:")
        print(tabulate(rows, headers=self.cursor.column_names))
        self.cursor.execute(activity_query)
        rows = self.cursor.fetchall()
        print("Activity table:")
        print(tabulate(rows, headers=self.cursor.column_names))
        self.cursor.execute(track_point_query)
        rows = self.cursor.fetchall()
        print("Track point table:")
        print(tabulate(rows, headers=self.cursor.column_names))
        
                                
def main():
    program = None
    create = True
    try:
        program = Part1()
        
        if create:
            program.reset_database()
            program.create_table_user()
            program.create_table_activity()
            program.create_table_track_point()
            # Check that the table is dropped
            program.show_tables()
            program.insert_gps_data()
        else:
            program.show_top_10_tables()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()

from DbConnector import DbConnector
from pprint import pprint 
import os
from datetime import datetime

class Part1:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def insert_gps_data(self):
        """
        Inserts the GPS data into the database
        """
        user_ids = os.listdir("./dataset/Data")
        
        # Ensures data is consistent if it's inserted again
        self.db["user"].drop()
        self.db["activity"].drop()
        self.db["track_point"].drop()
        
        user_collection = self.db["user"]
        activity_collection = self.db["activity"]
        track_point_collection = self.db["track_point"]
        
        # Index is necessary, because otherwise, $lookup will take forever
        track_point_collection.create_index("activity_id")
        
        with open("./dataset/labeled_ids.txt", "r") as f:
            user_labels = f.read().splitlines()
            
        for user_id in user_ids:
            print(f"Processing user {user_id}")
            user_root = f"./dataset/Data/{user_id}"
            has_label = 1 if user_id in user_labels else 0
            
            user = {"_id": user_id, "has_labels": has_label}
            user_collection.insert_one(user)

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
                    
                    # Insert activity and get activity id
                    
                    activity = {"user_id": user_id,
                                "transportation_mode": transportation_mode,
                                "start_date_time": datetime.strptime(start_date_time, "%Y-%m-%d %H:%M:%S"),
                                "end_date_time": datetime.strptime(end_date_time, "%Y-%m-%d %H:%M:%S")}
                    
                    activity_id = activity_collection.insert_one(activity).inserted_id
                    
                    # Convert track points to structured list of dictionaries
                    track_points_structured = [{"activity_id": activity_id,
                                                "lat": float(track_point[0]),
                                                "lon": float(track_point[1]),
                                                # Should in principle be int according to specification, but
                                                # is sometimes float, so convert float to int
                                                "altitude": int(float(track_point[3])),
                                                "date_days": float(track_point[4]),
                                                "date_time": datetime.strptime(f"{track_point[5]} {track_point[6]}", "%Y-%m-%d %H:%M:%S")}
                                                for track_point in track_points]
                    
                    track_point_collection.insert_many(track_points_structured)

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
        
    def print_collections_top10(self):
        """Prints the top 10 documents in each collection
        """
        collections = self.client['database'].list_collection_names()
        for collection_name in collections:
            collection = self.db[collection_name]
            documents = collection.find({}).limit(10)
            print(f"{collection_name} Collection:")
            for doc in documents: 
                pprint(doc)
        
                                
def main():
    program = None
    create = False
    try:
        program = Part1()
        
        if create:
            program.insert_gps_data()
        else:
            program.print_collections_top10()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()

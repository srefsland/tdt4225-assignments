from DbConnector import DbConnector
from tabulate import tabulate
from haversine import haversine, Unit, haversine_vector
import numpy as np
import time

class Part2:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
        
    def task1(self):
        query = """
        SELECT (SELECT COUNT(*) FROM user) AS user_count,
               (SELECT COUNT(*) FROM activity) AS activity_count,
               (SELECT COUNT(*) FROM track_point) AS trackpoint_count;
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print("Task 1: Number of users, activities and trackpoints in the database:")
        print(tabulate(rows, headers=self.cursor.column_names))
        
    def task2(self):
        query = """
        SELECT AVG(user_counts.trackpoint_count) AS average_count,
	           MIN(user_counts.trackpoint_count) AS minimum_count, 
	           MAX(user_counts.trackpoint_count) AS maximum_count
        FROM (
            SELECT COUNT(*) AS trackpoint_count
            FROM activity
            INNER JOIN track_point 
            ON activity.id = track_point.activity_id
            GROUP BY user_id
        ) AS user_counts;
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print("Task 2: Average, minimum and maximum number of trackpoints logged by the users:")
        print(tabulate(rows, headers=self.cursor.column_names))
    
    def task3(self):
        query = """
        SELECT user_id, COUNT(*) as activity_count
        FROM activity
        GROUP BY user_id
        ORDER BY activity_count DESC
        LIMIT 15;
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print("Task 3: Top 15 users with the most activities logged:")
        print(tabulate(rows, headers=self.cursor.column_names))
        
    def task4(self):
        query = """
        SELECT DISTINCT user_id
        FROM activity
        WHERE activity.transportation_mode = 'bus';
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print("Task 4: Users that have logged taking the bus:")
        print(tabulate(rows, headers=self.cursor.column_names))
        
    def task5(self):
        query = """
        SELECT user_id, COUNT(DISTINCT transportation_mode) as transportation_count
        FROM activity
        GROUP BY user_id
        ORDER BY transportation_count DESC
        LIMIT 10;
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print("Task 5: Top 10 users with most types of different transportation modes:")
        print(tabulate(rows, headers=self.cursor.column_names))
    
    def task6(self):
        # Assumes that an activity is logged twice if it has the same user_id, transportation_mode,
        # start_date_time and end_date_time
        query = """
        SELECT user_id, transportation_mode, start_date_time, end_date_time
        FROM activity
        GROUP BY user_id, transportation_mode, start_date_time, end_date_time
        HAVING COUNT(*) > 1;
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print("Task 6: Activities that are logged twice:")
        print(tabulate(rows, headers=self.cursor.column_names))
        
    def task7a(self):
        query = """
        SELECT COUNT(DISTINCT user_id)
        FROM activity
        WHERE DATEDIFF(end_date_time, start_date_time) = 1;
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print("Task 7a: Number of users with activities that end the next day:")
        print(tabulate(rows, headers=self.cursor.column_names))
        
    def task7b(self):
        query = """
        SELECT id, user_id, transportation_mode, TIMEDIFF(end_date_time, start_date_time) AS duration
        FROM activity
        WHERE DATEDIFF(end_date_time, start_date_time) = 1;
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print("Task 7b: Activities that end the next day:")
        print(tabulate(rows, headers=self.cursor.column_names))
        
    def task8(self):
        # Idea, find max and min lat and long for each activity and then compare activities between each user
        # Fetch all users
        self.cursor.execute("SELECT id FROM user")
        rows = self.cursor.fetchall()
        users = [user[0] for user in rows]
        
        close_users_set = set()
        # Find all pairs of activities that overlap in time and space, defined by time window and bounding boxes
        # (max lat and lon)
        activities_query = """
        SELECT u1.id AS user1_activity_id, u2.id AS user2_activity_id
        FROM (
            SELECT activity.id, MAX(lat) AS max_lat, MAX(lon) AS max_lon, MIN(lat) AS min_lat, MIN(lon) AS min_lon, start_date_time, end_date_time
            FROM activity
            INNER JOIN track_point
            ON activity.id = track_point.activity_id
            WHERE user_id = %(user_id1)s
            GROUP BY activity_id
        ) AS u1
        INNER JOIN (
            SELECT activity.id, MAX(lat) AS max_lat, MAX(lon) AS max_lon, MIN(lat) AS min_lat, MIN(lon) AS min_lon, start_date_time, end_date_time, user_id
            FROM activity
            INNER JOIN track_point
            ON activity.id = track_point.activity_id
            WHERE user_id = %(user_id2)s
            GROUP BY activity_id
        ) AS u2
        ON u1.max_lat >= u2.min_lat 
        AND u1.max_lon >= u2.min_lon 
        AND u1.min_lat <= u2.max_lat 
        AND u1.min_lon <= u2.max_lon 
        AND u1.end_date_time >= u2.start_date_time
        AND u1.start_date_time <= u2.end_date_time
        """
        select_trackpoints_query = """
        SELECT track_point.lat, track_point.lon, track_point.date_time
        FROM track_point
        WHERE track_point.activity_id = %(activity_id)s
        """

        # For each user, fetch their trackpoints and compare with those of every other user not in close_users_set
        for user_id1 in users:
            print(f"Comparing user {user_id1} with other users")
            for user_id2 in users:
                # No point in comparing previous users, or if the two users are both close to someone else
                if user_id2 > user_id1 and (user_id1 not in close_users_set or user_id2 not in close_users_set):
                    # Fetch trackpoints for user_id1
                    self._compare_users(user_id1, user_id2, activities_query, select_trackpoints_query, close_users_set)
            
        print(f"Task 8: Number of users that have been close to each other: {len(close_users_set)}")
    
    def _compare_users(self, user_id1, user_id2, activities_query, select_trackpoints_query, close_users_set):
        print(f"Comparing user {user_id1} with user {user_id2}")
        self.cursor.execute(activities_query, {'user_id1': user_id1, 'user_id2': user_id2})
        # The pairs of activities that overlap, we should only need to compare the pairs
        # respectively, as only they overlap in time and space
        rows = self.cursor.fetchall()
        activity_pairs = [(row[0], row[1]) for row in rows]
        
        for activity_pair in activity_pairs:
            # Fetch trackpoints for user_id1
            self.cursor.execute(select_trackpoints_query, {'activity_id': activity_pair[0]})
            rows = self.cursor.fetchall()
            user1_trackpoints = [(row[0], row[1], row[2]) for row in rows]
            # Fetch trackpoints for user_id2
            self.cursor.execute(select_trackpoints_query, {'activity_id': activity_pair[1]})
            rows = self.cursor.fetchall()
            user2_trackpoints = [(row[0], row[1], row[2]) for row in rows]
            
            if self._compare_trackpoints(user1_trackpoints, user2_trackpoints):
                print(f"User {user_id1} and user {user_id2} have been close to each other")
                close_users_set.add(user_id1)
                close_users_set.add(user_id2)
                return
                    
    def _compare_trackpoints(self, trackpoints1, trackpoints2):
        locs1 = [(trackpoint[0], trackpoint[1]) for trackpoint in trackpoints1]
        locs2 = [(trackpoint[0], trackpoint[1]) for trackpoint in trackpoints2]
        
        distances = haversine_vector(locs1, locs2, comb=True, unit=Unit.METERS)
        rows, columns = np.where(distances <= 50)
        
        for close_index_pair in zip(rows, columns):
            # A bit weird, but the columns of the distance matrix corresponds to user1
            # and the rows to user2
            trackpoint1 = trackpoints1[close_index_pair[1]]
            trackpoint2 = trackpoints2[close_index_pair[0]]
            time_difference = abs(trackpoint1[2] - trackpoint2[2]).total_seconds()
            # If the time difference is less or equal to 30 seconds, these users have been close to each other
            if time_difference <= 30:
                return True
        
        return False
            
    def task9(self):
        query = """
        WITH tp_cleaned AS (
            SELECT 
                id,
                activity_id,
                altitude,
                ROW_NUMBER() OVER (PARTITION BY activity_id) AS rn
            FROM track_point
            WHERE altitude != -777
        )

        SELECT user_id, SUM(
                CASE
                    WHEN tp.altitude > tp_prev.altitude THEN (tp.altitude - tp_prev.altitude) * 0.3048
                    ELSE 0
                END) AS total_gained_altitude
        FROM activity
        INNER JOIN tp_cleaned tp
        ON tp.activity_id = activity.id
        INNER JOIN tp_cleaned tp_prev
        ON tp.rn = tp_prev.rn + 1
        AND tp.activity_id = tp_prev.activity_id
        GROUP BY user_id
        ORDER BY total_gained_altitude DESC
        LIMIT 15;
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print("Task 9: Top 15 users with the highest total altitude gained:")
        print(tabulate(rows, headers=self.cursor.column_names))
        
    def task10(self):
        # Filter out users that have not labeled their activities,
        # no need to iterate through them
        user_query = """
        SELECT id
        FROM user
        WHERE has_labels = 1
        """
        self.cursor.execute(user_query)
        user_rows = self.cursor.fetchall()
        user_ids = [user_row[0] for user_row in user_rows]
        
        transport_query = """
        SELECT DISTINCT transportation_mode
        FROM activity
        WHERE transportation_mode IS NOT NULL
        """
        self.cursor.execute(transport_query)
        transportation_rows = self.cursor.fetchall()
        transportation_modes = [transport_row[0] for transport_row in transportation_rows]
        max_distances = {transportation_mode: (None, 0) for transportation_mode in transportation_modes}
        
        for user_id in user_ids:
            print(f"Calculating distances for user {user_id}")
            distances_user = {transport_mode: 0 for transport_mode in transportation_modes}
            distances = {}
            # Select all trackpoints for the user that have a transportation mode
            track_points_query = """
            SELECT activity.id, lat, lon, date_time, transportation_mode
            FROM activity
            INNER JOIN track_point
            ON activity.id = track_point.activity_id
            WHERE transportation_mode IS NOT NULL
            AND user_id = %(user_id)s
            """
            self.cursor.execute(track_points_query, {'user_id': user_id})
            trackpoint_rows = self.cursor.fetchall()
            for i in range(1, len(trackpoint_rows)):
                activity_id, lat, lon, date_time, transportation_mode = trackpoint_rows[i]
                prev_activity_id, prev_lat, prev_lon, prev_date_time, prev_transportation_mode = trackpoint_rows[i-1]
                # We only care about the actual date, not datetime
                date = date_time.date()
                prev_date = prev_date_time.date()
                # If the date is the same as the previous date, and the transportation mode is the same
                # as the previous transportation mode and the activity id is the same, we can calculate
                # the distance between the two points
                if date == prev_date and transportation_mode == prev_transportation_mode and activity_id == prev_activity_id:
                    distances[(date, transportation_mode)] = distances.get((date, transportation_mode), 0) + haversine((lat, lon), (prev_lat, prev_lon))
            
            for (date, transportation_mode), distance in distances.items():
                distances_user[transportation_mode] = max(distances_user[transportation_mode], distance)
        
            for transportation_mode, distance in distances_user.items():
                if distance > max_distances[transportation_mode][1]:
                    max_distances[transportation_mode] = (user_id, distance)
        
        print("Task 10: Users with the longest distance traveled per transportation mode:")
        for transportation_mode, (user_id, distance) in max_distances.items():
            print(f"Transportation mode {transportation_mode}: user: {user_id}, distance: {distance:.2f} km")
            
    def task11(self):
        query = """
        SELECT activity.user_id, COUNT(*) AS invalid_activity_count
        FROM ( 
            SELECT DISTINCT tp.activity_id
            FROM track_point AS tp
            INNER JOIN track_point AS tp_prev
            ON tp.id = tp_prev.id + 1
            AND tp.activity_id = tp_prev.activity_id
            WHERE ABS(TIMESTAMPDIFF(MINUTE, tp.date_time, tp_prev.date_time)) >= 5
        ) AS invalid_activities
        INNER JOIN activity
        ON invalid_activities.activity_id = activity.id
        GROUP BY activity.user_id;
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print("Task 11: Users with invalid activities:")
        print(tabulate(rows, headers=self.cursor.column_names))
    
    def task12(self):
        query = """
        SELECT user_id, transportation_mode
        FROM (
            SELECT user_id, transportation_mode,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY transportation_count DESC) AS ranking
            FROM (
                SELECT user_id, transportation_mode, COUNT(transportation_mode) AS transportation_count
                FROM activity
                WHERE transportation_mode IS NOT NULL
                GROUP BY user_id, transportation_mode
            ) AS transportation_counts
        ) AS transportation_ranking
        WHERE ranking = 1;
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print("Task 12: Most used transportation mode per user:")
        print(tabulate(rows, headers=self.cursor.column_names))

                                
def main():
    start_time = time.perf_counter()
    program = None
    try:
        program = Part2()
        # program.task1()
        # program.task2()
        # program.task3()
        # program.task4()
        # program.task5()
        # program.task6()
        # program.task7a()
        # program.task7b()
        # program.task8()
        # program.task9()
        # program.task10()
        # program.task11()
        program.task12()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()
    
    end_time = time.perf_counter()
    print(f"Program took {(end_time - start_time)/60} minutes to run")


if __name__ == '__main__':
    main()

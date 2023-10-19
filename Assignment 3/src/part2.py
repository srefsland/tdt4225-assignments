from DbConnector import DbConnector
from haversine import haversine, Unit, haversine_vector
import time
from pprint import pprint
import datetime


class Part2:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def task1(self):
        user_count = self.db.user.count_documents({})
        activity_count = self.db.activity.count_documents({})
        track_point_count = self.db.track_point.count_documents({})

        print("Task 1:")
        print(f"Number of users: {user_count}")
        print(f"Number of activities: {activity_count}")
        print(f"Number of trackpoints: {track_point_count}")

    def task2(self):
        count = self.db.activity.aggregate([
            {
                "$group": {
                    "_id": "$user_id",
                    "activity_count": {"$sum": 1}
                }
            },
            {
                "$group": {
                    "_id": None,
                    "average_activites": {"$avg": "$activity_count"}
                }
            }
        ])
        print("Task 2: Average activities per user")
        print(f"{list(count)[0].get('average_activites'): .2f}")

    def task3(self):
        users = self.db.activity.aggregate([
            {
                "$group": {
                    "_id": "$user_id",
                    "activity_count": {"$sum": 1}
                }
            },
            {
                "$sort": {"activity_count": -1}
            },
            {
                "$limit": 20
            }
        ])
        print("Task 3: Top 20 users with highest number of activities")
        self._print_results(users)

    def task4(self):
        users_that_have_taken_taxi = self.db.activity.distinct(
            "user_id", {"transportation_mode": "taxi"})

        print("Task 4: Users that have taken a taxi")
        self._print_results(users_that_have_taken_taxi)

    def task5(self):
        transportation_counts = self.db.activity.aggregate([
            {
                "$match": {"transportation_mode": {"$ne": None}}
            },
            {
                "$group": {
                    "_id": "$transportation_mode",
                    "activity_count": {"$sum": 1}
                }
            },
            {
                # Also sorting alphabetically to make it easier to read
                "$sort": {"_id": 1}
            }
        ])

        print("Task 5: Activity count of each transportation mode")
        self._print_results(transportation_counts)

    def task6a(self):
        year_with_most_activities = self.db.activity.aggregate([
            {
                "$project": {
                    "years": {
                        # Handle edge case where start and end date are in different years
                        "$cond": {
                            "if": {"$ne": [{"$year": "$start_date_time"}, {"$year": "$end_date_time"}]},
                            "then": [{"$year": "$start_date_time"}, {"$year": "$end_date_time"}],
                            "else": [{"$year": "$start_date_time"}]
                        }
                    }
                }
            },
            {
                # Creates two documents if start and end year are different
                "$unwind": "$years"
            },
            {
                # Then groups by year and counts the number of activities for each year
                # resulting in a document being counted twice if ends in a different year
                "$group": {
                    "_id": "$years",
                    "activity_count": {"$sum": 1}
                }
            },
            {
                "$sort": {"activity_count": -1}
            },
            {
                "$limit": 1
            }
        ])

        print("Task 6a: The year with most activities")
        self._print_results(year_with_most_activities)

    def task6b(self):
        year_with_most_hours = self.db.activity.aggregate([
            {
                "$project": {
                    "start_year": {"$year": "$start_date_time"},
                    "duration": {
                        "$divide": [
                            # Idea: start with minutes first (lowest precision for the date times, and then convert to hours)
                            {"$dateDiff": {"startDate": "$start_date_time",
                                           "endDate": "$end_date_time", "unit": "minute"}},
                            60.0
                        ]
                    }
                }
            },
            {
                "$group": {
                    "_id": "$start_year",
                    "total_hours": {"$sum": "$duration"}
                }
            },
            {
                "$sort": {"total_hours": -1}
            },
            {
                "$limit": 1
            }
        ])

        print("Task 6b: Year with most hours")
        self._print_results(year_with_most_hours)

    def task6b2(self):
        """Different implementation of task 6b using python instead of mongoDB,
        does handle the edge case where start and end date are in different years
        but the hours as largely the same
        """
        year_with_most_hours = self.db.activity.find({})

        recorded_hours_per_year = {}

        for activity in year_with_most_hours:
            if activity["start_date_time"].year != activity["end_date_time"].year:
                # Handle edge case where start and end date are in different years
                start_year = activity["start_date_time"].year
                end_year = activity["end_date_time"].year

                end_of_start_year = datetime.datetime(
                    start_year, 12, 31, 23, 59)
                beginning_of_end_year = datetime.datetime(end_year, 1, 1, 0, 0)

                duration_start_year = (
                    end_of_start_year - activity["start_date_time"]).total_seconds() / 3600
                duration_end_year = (
                    activity["end_date_time"] - beginning_of_end_year).total_seconds() / 3600

                recorded_hours_per_year[start_year] = recorded_hours_per_year.get(
                    start_year, 0) + duration_start_year
                recorded_hours_per_year[end_year] = recorded_hours_per_year.get(
                    end_year, 0) + duration_end_year
            else:
                duration = (
                    activity["end_date_time"] - activity["start_date_time"]).total_seconds() / 3600
                recorded_hours_per_year[activity["start_date_time"].year] = recorded_hours_per_year.get(
                    activity["start_date_time"].year, 0) + duration

        print("Task 6b: Year with most hours")
        # Print sorted dict
        recorded_hours_per_year = dict(
            sorted(recorded_hours_per_year.items(), key=lambda x: x[1], reverse=True)[:1])
        print(recorded_hours_per_year)

    def task7(self):
        activities = self.db.activity.aggregate([
            {
                "$addFields": {
                    # Basically select * + start_year
                    "start_year": {"$year": "$start_date_time"}
                }
            },
            {
                "$match": {
                    "user_id": "112",
                    "transportation_mode": "walk",
                    "start_year": 2008
                }
            },
            {
                "$lookup": {
                    "from": "track_point",
                    "localField": "_id",
                    "foreignField": "activity_id",
                    "as": "track_points"
                }
            }
        ])

        distance_in_km = 0

        for activity in activities:
            for i in range(1, len(activity["track_points"])):
                trackpoint = activity["track_points"][i]
                prev_trackpoint = activity["track_points"][i-1]
                
                if trackpoint["date_time"].year == 2008:
                    # Only count distance if trackpoint is in 2008
                    lat, lon = trackpoint["lat"], trackpoint["lon"]
                    prev_lat, prev_lon = prev_trackpoint["lat"], prev_trackpoint["lon"]

                    distance_in_km += haversine((lat, lon), (prev_lat, prev_lon))

        print("Task 7: Total distance walked by user 112 in 2008")
        print(f"{distance_in_km: .2f} km")

    def task8(self):
        activities = self.db.activity.aggregate([
            {
                "$lookup": {
                    "from": "track_point",
                    "localField": "_id",
                    "foreignField": "activity_id",
                    "as": "track_points"
                }
            },
            {
                "$project": {
                    "user_id": 1,
                    "track_points": {
                        # Filter out track points with altitude -777
                        "$filter": {
                            "input": "$track_points",
                            "as": "track_points",
                            "cond": {"$ne": ["$$track_points.altitude", -777]}
                        }
                    }
                }
            }
        ])

        gained_altitudes = {}

        for activity in activities:
            for i in range(1, len(activity["track_points"])):
                trackpoint = activity["track_points"][i]
                prev_trackpoint = activity["track_points"][i-1]

                if trackpoint["altitude"] > prev_trackpoint["altitude"]:
                    gained_altitudes[activity["user_id"]] = gained_altitudes.get(
                        activity["user_id"], 0) + (trackpoint["altitude"] - prev_trackpoint["altitude"]) * 0.3048

        print("Task 8: Top 20 users with highest gained altitude")
        # Get top 20 values from dict
        top_20_users = dict(sorted(gained_altitudes.items(),
                            key=lambda x: x[1], reverse=True)[:20])
        for user_id, gained_altitude in top_20_users.items():
            print(f"User {user_id}: {gained_altitude: .2f} meters")

    def task9(self):
        invalid_activity_count_users = {}

        activities = self.db.activity.aggregate([
            {
                "$lookup": {
                    "from": "track_point",
                    "localField": "_id",
                    "foreignField": "activity_id",
                    "as": "track_points"
                }
            }
        ])

        for activity in activities:
            if self._detect_invalid_activity(activity):
                invalid_activity_count_users[activity["user_id"]] = invalid_activity_count_users.get(
                    activity["user_id"], 0) + 1

        print("Task 9: Users with illegal activities")
        for user_id, invalid_activity_count in invalid_activity_count_users.items():
            print(f"User {user_id}: {invalid_activity_count} illegal activities")

    def _detect_invalid_activity(self, activity):
        """Helper function to determine if an activity is invalid.
        An activity is invalid if the time difference between two trackpoints is greater than 5 minutes

        Args:
            activity: the activity

        Returns:
            bool: true if activity is invalid, else false
        """
        for i in range(1, len(activity["track_points"])):
            trackpoint = activity["track_points"][i]
            prev_trackpoint = activity["track_points"][i-1]

            time_diff = (
                trackpoint["date_time"] - prev_trackpoint["date_time"]).total_seconds() / 60

            if time_diff > 5:
                return True

        return False

    def task10(self):
        users = self.db.user.find({})
        user_ids = [user["_id"] for user in users]
        forbidden_city_users = []

        for user_id in user_ids:
            if self._user_within_forbidden_city(user_id):
                print(f"User {user_id} has been in forbidden city at least once!")
                forbidden_city_users.append(user_id)

        print("Task 10: Users that have been in the forbidden city of Beijing")
        for user_id in forbidden_city_users:
            print(f"User {user_id}")

    def _user_within_forbidden_city(self, user_id):
        """Helper function to determine if a user has been within a certain radius of a coordinate 
        corresponding to the forbidden city of Beijing

        Args:
            user_id (string): the user id

        Returns:
            bool: true if user has been within the forbidden city, else false
        """
        forbidden_city_loc = (39.916, 116.397)
        radius = 1  # Radius in km
        activities = self.db.activity.aggregate([
            {
                "$match": {"user_id": user_id}
            },
            {
                "$lookup": {
                    "from": "track_point",
                    "localField": "_id",
                    "foreignField": "activity_id",
                    "as": "track_points"
                }
            }
        ])

        for activity in activities:
            for trackpoint in activity["track_points"]:
                distance = haversine(
                    (trackpoint["lat"], trackpoint["lon"]), forbidden_city_loc)

                if distance <= radius:
                    return True
        return False

    def task11(self):
        most_used_transportation_mode = self.db.activity.aggregate([
            {
                "$match": {
                    "transportation_mode": {"$ne": None}
                },
            },
            {
                "$group": {
                    "_id": {
                        "user_id": "$user_id",
                        "transportation_mode": "$transportation_mode"
                    },
                    "activity_count": {"$sum": 1}
                }
            },
            {
                "$sort": {
                    "_id.user_id": 1,
                    "activity_count": -1
                }
            },
            {
                "$group": {
                    "_id": "$_id.user_id",
                    "most_used_transportation_mode": {"$first": "$_id.transportation_mode"}
                }
            },
            {
                "$sort": {"_id": 1}
            }
        ])

        print("Task 11: Most used transportation mode for each user")
        self._print_results(most_used_transportation_mode)

    def _print_results(self, results):
        """Pretty prints the results as obtained from queries

        Args:
            results: the queried results
        """
        for document in results:
            pprint(document)


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
        # program.task6a()
        # program.task6b()
        # program.task6b2()
        program.task7()
        # program.task8()
        # program.task9()
        # program.task10()
        # program.task11()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()

    end_time = time.perf_counter()
    print(f"Program took {(end_time - start_time)/60} minutes to run")


if __name__ == '__main__':
    main()

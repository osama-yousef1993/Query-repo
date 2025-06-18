from src_.cloud.gcp import GCPCloud
from src_.helpers.tf_idf import TFIDFProcess


class ProcessData:
    def __init__(self):
        self.fs_client = GCPCloud()

    def recommended_places(self, user_id):
        user_data = self.fs_client.get_user_data(user_id)
        trips = user_data["tripsIds"]
        trip_dict = dict()
        for trip in trips:
            visit_places = list()
            trip_data = self.fs_client.get_trip_data(trip)
            for des_id in trip_data["destinationsIds"]:
                destination_data = self.fs_client.get_destination_data(des_id)
                for place in destination_data["visitedPlaces"]:
                    visit_places.append(place)
            trip_dict_res = self.Map_place(visit_places)
            trip_dict[trip_data["name"]] = dict()
            trip_dict[trip_data["name"]]["visited_places"] = trip_dict_res
            destination_places = self.get_all_places()
            recommend_result = TFIDFProcess().build_recommended_places(
                destination_places, trip_dict_res
            )
            trip_dict[trip_data["name"]]["recommended_places"] = recommend_result
        return trip_dict

    def Map_place(self, places):
        places_data = list()
        for place in places:
            place_dict = dict()
            place_dict["Name"] = place["name"]
            place_dict["Rating"] = place["rating"] if place["rating"] else 0
            # place_dict["Description"] = place["description"]
            place_dict["Address"] = place["address"]
            place_dict["Type"] = ",".join(place["types"])
            place_dict["Reviews"] = ",".join(place["address"]) if "Reviews" in place.keys() else ""
            places_data.append(place_dict)
        return places_data

    def get_all_places(self):
        places_data = self.fs_client.get_destinations()
        visited_places = list()
        for desc in places_data:
            for visited in desc["visitedPlaces"]:
                visited_places.append(visited)
        places_res = self.Map_place(visited_places)
        return places_res

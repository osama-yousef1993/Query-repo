from src.database.gcp import GCP
from src.helpers.td_idf import Recommendation
from src.helpers.tf_idf import NewTFIDF


class Controllers:
    def response(self, user_id):
        td_idf = Recommendation().clean_df()

        result = Recommendation().buildARecommender(td_idf)
        # result = TDIDF().new_build_recommender(td_idf)

        return result

    def build_rows(self):
        tf_idf_data = Recommendation().process()
        result = tf_idf_data
        print(type(tf_idf_data[0]))
        fs = GCP()
        fs.insert_data(tf_idf_data)
        return result

    def get_rows(self):
        fs = GCP()
        result = fs.get_fs_data()
        response = list()
        for doc in result:
            response.append(doc.to_dict())
        return response


class TFIDFControllers:
    def get_recommended(self):
        tf_idf_result = NewTFIDF().build_recommended()
        return tf_idf_result

    def get_recommended_by_user_id(self, user_id):
        tf_idf_result = NewTFIDF().build_recommended_by_user_id(user_id)
        return tf_idf_result


class BuildData:
    def insert_data(self):
        tf_idf = NewTFIDF()
        fs = GCP()
        # user_data = tf_idf.build_user_data()
        places_data = tf_idf.build_places_data()
        # fs.insert_user_data(user_data)
        fs.insert_places_data(places_data)
        return {"status": 201}

    def map_users(self, user_id):
        fs = GCP()
        users = fs.read_new_users(user_id)
        users_list = list()
        for user in users:
            users_list.append(user.to_dict())
        return users_list

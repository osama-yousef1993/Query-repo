import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


from src.database.gcp import GCP


class NewTFIDF:
    def __init__(self):
        self.df = pd.read_excel("merged_data_with_attractions.xlsx")

    def build_recommended(self):
        df = self.clean_data(self.df)
        # user trip
        user_df = df.head(5)
        # FS data
        df = df.tail(1000)
        data = self.build_similarity(df, user_df)
        recommended = self.get_recommended(df, data)
        return recommended

    def build_recommended_by_user_id(self, user_id):
        # todo get user and places data from fs
        user_data = GCP().get_user_data(user_id)
        response = user_data.to_dict()
        places_data = GCP().get_places_data()
        places = list()
        for place in places_data:
            places.append(place.to_dict())
        df = self.clean_data(pd.DataFrame(places))
        user_df = pd.DataFrame(response["trips"])
        # df = df.tail(1000)
        data = self.build_similarity(df, user_df)
        recommended = self.get_recommended(df, data)
        return recommended

    def clean_data(self, df):
        df = df.drop_duplicates(keep="last")
        df = df.dropna(axis=0)
        df["Type"] = df["Type"].str.replace("_", " ")

        return df

    def build_similarity(self, df, user_df):
        # FS DataFrame => Recommended Places
        df["TextData"] = (
            df["Type"] + " " + df["Reviews"] + " " + df["Rating"].astype(str)
        )
        #  User data => User selected trips
        user_df["TextData"] = (
            user_df["Type"]
            + " "
            + user_df["Reviews"]
            + " "
            + user_df["Rating"].astype(str)
        )
        # Create the TF-IDF vectorizer
        tfidf = TfidfVectorizer(stop_words="english", token_pattern=r"\b[a-zA-Z0-9]+\b")
        # Compute the TF-IDF matrix
        tfidf_matrix = tfidf.fit_transform(df["TextData"])
        tfidf_matrix_user = tfidf.transform(user_df["TextData"])
        data = dict()
        # i = => one loop will loop for inner loop 1000 times
        for idx_user, user_row in enumerate(tfidf_matrix_user):
            sim_result = list()
            # i = 1000
            for idx, row in enumerate(tfidf_matrix):
                cosine_sim = cosine_similarity(row, user_row)
                sim_result.append({idx: cosine_sim[0, 0]})
            sim_result = sorted(
                sim_result, key=lambda x: list(x.values())[0], reverse=True
            )
            # each place will has array with 1000 records => records will be dict type with key and value
            data[idx_user] = sim_result
        return data

    def get_recommended(self, df, data):
        unqiue_palces = list()
        recommendations = list()
        places_indices = list()
        # user data
        for key, value in data.items():
            # loop sim places
            for item in value:
                # item dict
                for k, v in item.items():
                    places_indices.append(k)

        recommended_places = df.iloc[places_indices].to_dict(orient="records")
        for recommend_place in recommended_places:
            if recommend_place["Name"] not in unqiue_palces:
                unqiue_palces.append(recommend_place["Name"])
                data = dict()
                data["Name"] = recommend_place["Name"]
                data["Address"] = recommend_place["Address"]
                data["Type"] = recommend_place["Type"]
                data["Rating"] = recommend_place["Rating"]
                data["Category"] = recommend_place["Category"]
                data["Reviews"] = recommend_place["Reviews"]
                recommendations.append(data)
                if len(recommendations) >= 20:
                    break
        return recommendations

    def build_user_data(self):
        df = self.clean_data(self.df)
        user_df = df.head(10)
        result = list()
        unique_list = list()
        user_data = dict()
        user_data["name"] = "Osama"
        user_data["email"] = "Osama@hotmail.com"
        user_data["userName"] = "OsamaYousef"
        user_data["address"] = "jordan"

        for _, rows in user_df.iterrows():
            if rows["Name"] not in unique_list:
                unique_list.append(rows["Name"])
                data = dict()
                data["Name"] = rows["Name"]
                data["Address"] = rows["Address"]
                data["Type"] = rows["Type"]
                data["Rating"] = rows["Rating"]
                data["Category"] = rows["Category"]
                data["Reviews"] = rows["Reviews"]
                result.append(data)
        user_data["trips"] = result
        return user_data

    def build_places_data(self):
        df = self.clean_data(self.df)
        df = df.tail(-10)
        result = list()
        unique_list = list()
        for _, rows in df.iterrows():
            if rows["Name"] not in unique_list:
                unique_list.append(rows["Name"])
                data = dict()
                data["Name"] = rows["Name"]
                data["Address"] = rows["Address"]
                data["Type"] = rows["Type"]
                data["Rating"] = rows["Rating"]
                data["Category"] = rows["Category"]
                data["Reviews"] = rows["Reviews"]
                result.append(data)
        return result

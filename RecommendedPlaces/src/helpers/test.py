import json

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import MinMaxScaler

from src.database.gcp import GCP


class TFIDFClass:
    def __init__(self):
        self.gcp = GCP()
        self.data = self.get_data()
        self.df = self.build_data_frame(self.data)

    def get_data(self):
        result = self.gcp.get_fs_data()
        fs_data = list()
        for doc in result:
            fs_data.append(doc.to_dict())
        return fs_data

    # def build_data_frame(self, data):
    #     df = pd.DataFrame(data)
    #     return df

    def build_data_frame(self, data):
        # Load the Dataset
        data = sorted(data, key=lambda x: x["Rating"], reverse=True)
        df = pd.DataFrame(data)
        # fixing the data
        df = df[~df["Name"].duplicated(keep="last")]
        df = df.drop_duplicates(keep="first")
        df = df[~df["Address"].isna()]
        df = df[~df["Type"].isna()]
        df = df[~df["Rating"].isna()]
        df = df[~df["Category"].isna()]
        df = df[~df["Reviews"].isna() & (df["Reviews"] != "") & (df["Reviews"] != ", ")]
        # Remove underscores from the 'Type' column
        df["Type"] = df["Type"].str.replace("_", " ")

        data = df.to_dict(orient="records")
        with open("new_files.json", "w") as f:
            f.write(json.dumps(data))

        return df

    def buildARecommender(self):
        df = self.df
        # Concatenate text from 'Type', 'Category', and 'Reviews' columns
        scaler = MinMaxScaler()
        # processed_reviews = processed_reviews.str.replace(r"\b\w{1,2}\b", "")
        df["Rating_normalized"] = scaler.fit_transform(df[["Rating"]])
        df["TextData"] = f"{df['Type']} {df['Reviews']} {df['Rating_normalized']}"

        # Create the TF-IDF vectorizer
        tfidf = TfidfVectorizer(stop_words="english", token_pattern=r"\b[a-zA-Z0-9]+\b")

        # Compute the TF-IDF matrix
        tfidf_matrix = tfidf.fit_transform(df["TextData"])

        # Compute the cosine similarity matrix
        cosine_sim = cosine_similarity(tfidf_matrix, tfidf_matrix)

        # Reset the index of the DataFrame
        df = df.reset_index(drop=True)

        # Iterate over each row in the dataset
        recommendations = []
        unique_places = []
        try:
            for idx in range(len(df)):
                # Get the similarity scores for this place
                sim_scores = list(enumerate(cosine_sim[idx]))
                # Sort the places based on the similarity scores
                sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
                # Get the top N recommendations
                top_n = 16
                sim_scores = sim_scores[1:top_n]  # Exclude the input place itself
                place_indices = [score[0] for score in sim_scores]
                # Get the names of the recommended places
                recommended_places = df.iloc[place_indices].to_dict(orient="records")
                # Store the unique recommendations in a list
                for recommend_place in recommended_places:
                    if recommend_place["Name"] not in unique_places:
                        unique_places.append(recommend_place["Name"])
                        data = dict()
                        data["Name"] = recommend_place["Name"]
                        data["Address"] = recommend_place["Address"]
                        data["Type"] = recommend_place["Type"]
                        data["Rating"] = recommend_place["Rating"]
                        data["Category"] = recommend_place["Category"]
                        data["Reviews"] = recommend_place["Reviews"]
                        recommendations.append(data)
        except Exception as e:
            print(e)
        try:
            recommendations = sorted(
                recommendations, key=lambda x: x["Rating"], reverse=True
            )
            with open("Recommended.json", "w") as f:
                f.write(json.dumps(recommendations))
        except Exception as e:
            print(e)

        # Save the DataFrame to a new Excel file
        return recommendations

import json

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import MinMaxScaler


class Recommendation:
    # __init__
    # clean_df
    # buildARecommender
    def __init__(self):
        self.df = pd.read_excel("merged_data_with_attractions.xlsx")

    def process(self):
        df = self.df
        df.columns = ["Name", "Address", "Type", "Rating", "Category", "Reviews"]
        result = []
        unique_list = []
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

    def clean_df(self):
        # Load the Dataset
        data = sorted(
            self.df.to_dict(orient="records"), key=lambda x: x["Rating"], reverse=True
        )
        df = pd.DataFrame(data)
        # df = self.df
        # fixing the data
        # df = df[~df["Name"].duplicated(keep="last")]
        df = df.drop_duplicates(keep="last")
        df = df.dropna(axis=0)
        # df = df[~df["Address"].isna()]
        # df = df[~df["Type"].isna()]
        # df = df[~df["Rating"].isna()]
        # df = df[~df["Category"].isna()]
        # df = df[~df["Reviews"].isna()]
        # df = df[~df["Reviews"].isna() & (df["Reviews"] != "") & (df["Reviews"] != ", ")]

        # Remove underscores from the 'Type' column
        df["Type"] = df["Type"].str.replace("_", " ")

        data = df.to_dict(orient="records")
        with open("new_files2.json", "w") as f:
            f.write(json.dumps(data))

        return df

    def buildARecommender(self, df):
        # Concatenate text from 'Type', 'Category', and 'Reviews' columns
        scaler = MinMaxScaler()
        # processed_reviews = processed_reviews.str.replace(r"\b\w{1,2}\b", "")
        df["Rating_normalized"] = scaler.fit_transform(df[["Rating"]])
        df["TextData"] = f"{df['Type']} {df['Reviews']} {df['Rating_normalized']}"

        # Create the TF-IDF vectorizer
        tfidf = TfidfVectorizer(stop_words="english", token_pattern=r"\b[a-zA-Z0-9]+\b")

        # Compute the TF-IDF matrix
        tfidf_matrix = tfidf.fit_transform(df["TextData"])
        tfidf_matrix1 = tfidf.fit_transform(df)

        try:
            for vector in tfidf_matrix1:
                # Compute the cosine similarity matrix
                cosine_sim = cosine_similarity(tfidf_matrix, vector)
        except Exception as e:
            print(e)
        # Reset the index of the DataFrame
        df = df.reset_index(drop=True)

        # Iterate over each row in the dataset
        recommendations = []
        unique_places = []
        try:
            for idx in df.index:
                # Get the similarity scores for this place
                sim_scores = list(enumerate(cosine_sim[idx]))
                # Sort the places based on the similarity scores
                sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
                # Get the top N recommendations
                top_n = 6
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
                recommendations, lambda x: x["Rating"], reverse=True
            )
            with open("Recommended.json", "w") as f:
                f.write(json.dumps(recommendations))
        except Exception as e:
            print(e)

        # Save the DataFrame to a new Excel file
        return recommendations

    # def new_build_recommender(self, df):
    #     processed_reviews = df["Reviews"].str.lower()
    #     scaler = MinMaxScaler()
    #     # processed_reviews = processed_reviews.str.replace(r"\b\w{1,2}\b", "")
    #     df["Rating_normalized"] = scaler.fit_transform(df[["Rating"]])

    #     features = pd.concat(
    #         [processed_reviews, df["Type"], df["Rating_normalized"]], axis=1
    #     )

    #     tfidf_vectorizer = TfidfVectorizer(stop_words="english")
    #     tfidf_matrix = tfidf_vectorizer.fit_transform(features)

    #     cosine_sim = cosine_similarity(tfidf_matrix, tfidf_matrix)

    #     # Print similarity scores for each place
    #     for idx, place in enumerate(df["Name"]):
    #         sim_scores = cosine_sim[idx]
    #         print(f"Similarity scores for {place}: {sim_scores}")

    #     recommendations = []
    #     unique_places = []
    #     try:
    #         print(len(df))
    #         for idx in range(len(df)):
    #             if idx >= len(list(enumerate(cosine_sim))):
    #                 break
    #             sim_scores = list(enumerate(cosine_sim[idx]))

    #             sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)

    #             top_n = 11
    #             sim_scores = sim_scores[1:top_n]
    #             place_indices = [i[0] for i in sim_scores]

    #             # recommendation_place = df.loc[place_indices, "Name"]

    #             recommendation_place = df.iloc[place_indices].to_dict(orient="records")
    #             # data = dict()
    #             # # 'Name', 'Address', 'Type', 'Rating', 'Category', 'Reviews'
    #             # data["Name"] = recommendation_place.values[0]
    #             # data["Address"] = recommendation_place.values[1]
    #             # data["Type"] = recommendation_place.values[2]
    #             # data["Rating"] = recommendation_place.values[3]
    #             # data["Category"] = recommendation_place.values[4]
    #             # data["Reviews"] = recommendation_place.values[5]
    #             # print(data)
    #             # recommendations.append(recommendation_place)
    #             # recommendations.append(recommendation_place)
    #             for recommend_place in recommendation_place:
    #                 if recommend_place["Name"] not in unique_places:
    #                     unique_places.append(recommend_place["Name"])
    #                     recommendations.append(recommend_place)
    #     except Exception as e:
    #         print(e)
    #     with open("recommendations.json", "w") as f:
    #         json.dump(recommendations, f, indent=4)
    #     recommendations = sorted(
    #         recommendations, key=lambda x: x["Rating_normalized"], reverse=True
    #     )
    #     return recommendations


pip install pandas scikit-learn

import json

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

def read_data():
    df = pd.read_excel("merged_data_with_attractions.xlsx")
    user_df = pd.read_excel("merged_data_with_attractions.xlsx")
    # second DataFrame from user data
    return df, user_df

def clean_data(df, user_df):
    df = df.drop_duplicates(keep="last")
    df = df.dropna(axis=0)
    df["Type"] = df["Type"].str.replace("_", " ")
    user_df = user_df.drop_duplicates(keep="last")
    user_df = user_df.dropna(axis=0)
    user_df["Type"] = user_df["Type"].str.replace("_", " ")
    
    return df, user_df

df, user_df = read_data()
new_df, new_user_df = clean_data(df, user_df)

user_df = new_df.head(5)
user_df.head(10)

df = new_df.tail(1000)
df.head(5)

def build_similarity(df, user_df):
    df["TextData"] = df['Type'] + " " + df["Reviews"] + " " + df["Rating"].astype(str)
    user_df["TextData"] = user_df["Type"] + " " + user_df["Reviews"] + " " + user_df["Rating"].astype(str)
    # Create the TF-IDF vectorizer
    tfidf = TfidfVectorizer(stop_words="english", token_pattern=r"\b[a-zA-Z0-9]+\b")
    # Compute the TF-IDF matrix
    tfidf_matrix = tfidf.fit_transform(df["TextData"])
    tfidf_matrix_user = tfidf.transform(user_df["TextData"])
    print(len(tfidf_matrix.toarray()[0]))
    print(len(tfidf_matrix_user.toarray()[0]))
    data = dict()
    for idx_user, user_row in enumerate(tfidf_matrix_user):
        sim_result = list()
        for idx, row in enumerate(tfidf_matrix):
            cosine_sim = cosine_similarity(row, user_row)
            sim_result.append({idx: cosine_sim[0, 0]})
        # print(f" sim_result with idx_user {idx_user} => >> {sim_result}")
        sim_result = sorted(sim_result, key=lambda x: list(x.values())[0], reverse=True)
        # print(sim_result)
        data[idx_user] = sim_result
    return data

data = build_similarity(df, user_df)
data



def get_recommended(data):
    unqiue_palces = list()
    recommendations = list()
    places_indices = list()
    for key, value in data.items():
        for item in value[0: 3]:
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
    return recommendations

res = get_recommended(data)
print(res)
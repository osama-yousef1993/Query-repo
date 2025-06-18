from flask import Flask, abort, jsonify

from src.controllers.controllers import BuildData, Controllers, TFIDFControllers

app = Flask(__name__)
app.secret_key = "frY^eN&UB4yZgVt+Isbjq%deH"


@app.route("/get_recommended_places/<user_id>")
def get_recommended_places(user_id):
    try:
        result = Controllers().response(user_id)
        return jsonify(result)
    except Exception:
        abort(500)


@app.route("/insert_rows")
def insert_rows():
    try:
        result = Controllers().build_rows()
        return jsonify(result)
    except Exception as e:
        print(f"error in response data :=> {e}")
        abort(500)


@app.route("/get_rows")
def get_rows():
    try:
        result = Controllers().get_rows()
        return jsonify(result)
    except Exception as e:
        print(f"error getting data from FS :=> {e}")
        abort(500)


@app.route("/get_recommended")
def get_recommended():
    try:
        result = TFIDFControllers().get_recommended()
        return jsonify(result)
    except Exception as e:
        print(f"error getting recommended places {e}")
        abort(500)


@app.route("/get_recommended_by_user_id/<user_id>")
def get_recommended_by_user_id(user_id):
    try:
        result = TFIDFControllers().get_recommended_by_user_id(user_id)
        return jsonify(result)
    except Exception as e:
        print(f"error getting recommended places {e}")
        abort(500)


@app.route("/build_data")
def build_data():
    try:
        res = BuildData().insert_data()
        return jsonify(res)
    except Exception as e:
        print(e)
        abort(500)


@app.route("/users/<user_id>")
def users(user_id):
    try:
        res = BuildData().map_users(user_id)
        return jsonify(res)
    except Exception as e:
        print(e)
        abort(500)


if __name__ == "__main__":
    app.run(debug=True)

from flask import Flask, jsonify

from src_.controllers.process import ProcessData

app = Flask(__name__)

app.secret_key = "frY^eN&UB4yZgVt+Isbjq%deH"


@app.route("/recommended_places/<user_id>")
def get_recommended_places(user_id):
    try:
        result = ProcessData().recommended_places(user_id)
        return jsonify(result)
    except Exception as e:
        res = {"status": 500, "error": f"Error Occur {e}"}
        return jsonify(res)


if __name__ == "__main__":
    app.run(debug=True)

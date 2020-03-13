from flask import Flask, make_response, jsonify

def create_app():
	app = Flask(__name__)
	app.config['count'] = 0
	return app


app = create_app()

@app.route('/test', methods=['GET'])
def count_requests():
	app.config['count'] = app.config['count'] + 1
	return make_response(jsonify(app.config["count"]), 200)

if __name__ == '__main__':
	app.run(debug=True)
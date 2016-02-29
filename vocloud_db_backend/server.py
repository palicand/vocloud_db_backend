import sys
from flask import Flask, request, jsonify
import vocloud_db_backend.cass_db as db
import argparse
import json
import logging
import os

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("vocloud_db_backend")


@app.route('/spectrum', methods=["POST"], defaults={"spectrum_id": None})
@app.route('/spectrum/<spectrum_id>', methods=["GET", "POST"])
def spectrum(spectrum_id):
    if request.method == "POST" and spectrum_id is None:
        data = request.get_json()
        created = db.insert_spectrum(data)
        return jsonify({"result":"success", "data": created}), 201
    elif request.method == "GET" and spectrum_id is not None:
        data_from_db = db.get_spectrum(spectrum_id)
        if data_from_db is not None:
            return jsonify({"result":"success", "data": data_from_db}), 200
        else:
            return jsonify({"result":"not_found", "data": []}), 404
    else:
        return jsonify({"result": "unsupported_method", "data": []}), 403

def argument_parse(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("db_config")
    return parser.parse_args(argv)

def main(argv):
    args = argument_parse(argv)
    db_config = args.db_config
    logger.info("Loading config from %s", db_config)
    with open(db_config, mode="r") as conf_file:
        conf = json.load(conf_file)
        logging.info("Database config %s", conf)
        db.connect(conf["hosts"], conf["port"])
    return app

if __name__ == '__main__':
    main(sys.argv[1:])

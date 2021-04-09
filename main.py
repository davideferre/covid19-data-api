from datetime import datetime, timedelta
from flask import Flask, request, jsonify
from influxdb import InfluxDBClient

_INFLUXDB_HOST = '127.0.0.1'
_INFLUXDB_PORT = 8086
_INFLUXDB_DBNAME = 'dati'

app = Flask(__name__)

@app.route('/api/v1/data/nation')
def nation():
    date_from = request.args.get('from', default=None)
    date_to = request.args.get('to', default=None)
    return _get_data('nation', date_from, date_to)


@app.route('/api/v1/data/region')
def region():
    codice_regione = request.args.get('regione', default=None)
    if codice_regione is None:
        return jsonify({'error': "'regione' is required field"}), 400
    filters=[{'codice_regione': codice_regione}]
    date_from = request.args.get('from', default=None)
    date_to = request.args.get('to', default=None)
    return _get_data('region', date_from, date_to, filters)


@app.route('/api/v1/data/province')
def province():
    codice_regione = request.args.get('regione', default=None)
    sigla_provincia = request.args.get('provincia', default=None)
    if codice_regione is None and sigla_provincia is None:
        return jsonify({'error': "'regione' or 'provincia' are required fields"}), 400
    filters = [
        {'codice_regione': codice_regione},
        {'sigla_provincia': sigla_provincia},
    ]
    date_from = request.args.get('from', default=None)
    date_to = request.args.get('to', default=None)
    return _get_data('province', date_from, date_to, filters)


def _get_data(measurement, date_from=None, date_to=None, filters=None):
    query = "SELECT * FROM " + measurement + " WHERE 1=1"
    if date_from is not None:
        date_from_obj = datetime.strptime(date_from, '%Y-%m-%d')
        query += " AND time >= '" + date_from_obj.isoformat() + "Z'"
    if date_to is not None:
        date_to_obj = datetime.strptime(date_to, '%Y-%m-%d')
        date_to_obj = date_to_obj + timedelta(days=1)
        query += " AND time < '" + date_to_obj.isoformat() + "Z'"
    for filter in filters:
        for key in filter.keys():
            if filter[key] is not None:
                query += " AND " + key + " = '" + filter[key] + "'"
    query +=" ORDER BY time DESC"
    client = InfluxDBClient(host=_INFLUXDB_HOST, port=_INFLUXDB_PORT)
    client.switch_database(_INFLUXDB_DBNAME)
    nation_datas = client.query(query).get_points()
    client.close()
    result = []
    for data in nation_datas:
        result.append(data)
    return jsonify(result)

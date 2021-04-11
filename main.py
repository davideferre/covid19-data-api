from datetime import datetime, timedelta
from flask import Flask, request, jsonify
from flask_cors import CORS
from influxdb import InfluxDBClient

_INFLUXDB_HOST = '127.0.0.1'
_INFLUXDB_PORT = 8086
_INFLUXDB_DBNAME = 'dati'

app = Flask(__name__)
CORS(app)

@app.route('/api/v1/data/nations')
def nation():
    date_from = request.args.get('from', default=None)
    date_to = request.args.get('to', default=None)
    return _get_data('nation', date_from, date_to)


@app.route('/api/v1/data/regions')
def region():
    region_code = request.args.get('region', default=None)
    if region_code is None:
        return jsonify({'error': "'region' is required field"}), 400
    filters=[{'codice_regione': region_code}]
    date_from = request.args.get('from', default=None)
    date_to = request.args.get('to', default=None)
    return _get_data('region', date_from, date_to, filters)


@app.route('/api/v1/data/provinces')
def province():
    region_code = request.args.get('region', default=None)
    province_abbr = request.args.get('province', default=None)
    if region_code is None and province_abbr is None:
        return jsonify({'error': "'regione' or 'provincia' are required fields"}), 400
    filters = [
        {'codice_regione': region_code},
        {'codice_provincia': province_abbr},
    ]
    date_from = request.args.get('from', default=None)
    date_to = request.args.get('to', default=None)
    return _get_data('province', date_from, date_to, filters)


def _get_data(measurement, date_from=None, date_to=None, filters=None):
    query = ("SELECT * FROM %s WHERE 1=1") % (measurement)
    params = {}
    if date_from is not None:
        date_from_obj = datetime.strptime(date_from, '%Y-%m-%d')
        query += " AND time >= '" + date_from_obj.isoformat() + "Z'"
    if date_to is not None:
        date_to_obj = datetime.strptime(date_to, '%Y-%m-%d')
        date_to_obj = date_to_obj + timedelta(days=1)
        query += " AND time < '" + date_to_obj.isoformat() + "Z'"
    if filters is not None:
        for filter in filters:
            for key in filter.keys():
                if filter[key] is not None:
                    query += " AND " + key +" = $" + key
                    params[key] = filter[key]
    query +=" ORDER BY time ASC"
    client = InfluxDBClient(host=_INFLUXDB_HOST, port=_INFLUXDB_PORT)
    client.switch_database(_INFLUXDB_DBNAME)
    influx_datas = client.query(query, bind_params=params).get_points()
    client.close()
    result = []
    for data in influx_datas:
        data['id'] = str(data['time'])
        if measurement != 'nation':
            data['id'] += '-' + str(data['codice_regione'])
        if measurement == 'province':
            data['id'] += '-' + str(data['codice_provincia'])
        result.append(data)
    result = {measurement + 's': result}
    return jsonify(result)

from flask import Flask, render_template
import os
import json

app = Flask(__name__)

CONFIG_DIR = os.path.join(os.getcwd(), 'configs', 'geotifs')


# Constants for all config files
DEFAULT_ABOUT = {
    "Title": "Geo Entity Stats Generation",
    "Description": "Geo Entity Stats Generation for Configured Parameters",
    "Divison": "GAWG, EPSA, SAC",
    "Author": "Nitin Mishra",
    "Reviewed By": "Pankaj Bodani",
    "Maintained By": "Mihir, Harish"
}

DEFAULT_GLOBAL_PARAM = {
    "database": {
        "host": "192.168.2.149",
        "username": "postgres",
        "password": "Vedas@123",
        "port": 5433,
        "db": "geoentity_stats",
        "geoentity_table": "geoentity",
        "geoentity_stats_table": "geoentity_param_time_stat",
        "param_table": "parameters",
        "processing_record_chunk": 50000
    }
}

CONFIG_MAP = {
    "mapping_type": "entity_mapping",
    "mapping_keys_for_stats_gen": [

    ],
    "mapping": {
        "entity_mapping": {
            
        }
    },
    "max_processes": 8,
    "processes_for_param_group": 3,
    "param_template": {}
}

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/list/key=<int:key_id>')
def parameter_page(key_id):
    return render_template('parameter_id.html', key_id=key_id)


@app.route('/register/<param_name>', methods=['GET', 'POST'])
def register_parameter(param_name):
    # Ensure the config directory exists
    os.makedirs(CONFIG_DIR, exist_ok=True)

    # Path to the config file
    config_path = os.path.join(CONFIG_DIR, f'{param_name}.json')

    # If the config file doesn't exist, create it with default structure
    if not os.path.exists(config_path):
        initial_data = {
            "about": DEFAULT_ABOUT,
            "global_param": DEFAULT_GLOBAL_PARAM,
            "config": CONFIG_MAP
        }
        with open(config_path, 'w') as f:
            json.dump(initial_data, f, indent=4)

    # Load existing config file
    with open(config_path, 'r') as f:
        config_data = json.load(f)

    # if request.method == 'POST':
        


    return render_template('register.html', param_name=param_name)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5004)
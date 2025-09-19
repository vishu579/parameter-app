from flask import Flask, render_template, request, redirect, url_for, jsonify
import os
import json
import paramiko
import subprocess
import sqlite3
import threading
import time

app = Flask(__name__)

CONFIG_DIR = os.path.join(os.getcwd(), 'configs', 'geotifs')

DB_PATH = 'process_status.db'

SSH_IP= '192.168.2.67'
SSH_USER= 'isro'
SSH_PASS= 'admin@123'

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


def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS process_status (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            param_name TEXT UNIQUE,
            status TEXT,
            pid INTEGER,
            start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            end_time TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

init_db()

def update_status(param_name, status, pid=None, end_time=None):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    if status == 'running':
        c.execute('''
            INSERT OR REPLACE INTO process_status (param_name, status, pid, start_time, end_time)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP, NULL)
        ''', (param_name, status, pid))
    elif status in ('completed', 'failed'):
        c.execute('''
            UPDATE process_status SET status = ?, end_time = CURRENT_TIMESTAMP WHERE param_name = ?
        ''', (status, param_name))
    else:
        # For other status updates
        c.execute('''
            UPDATE process_status SET status = ? WHERE param_name = ?
        ''', (status, param_name))

    conn.commit()
    conn.close()

def run_process_in_background(file_path, name):
    def target():
        try:
            # Start the process
            process = subprocess.Popen(['python', 'GeoEntity_Stats_Generation_Recursive_Forecast.py', file_path])
            update_status(name, 'running', pid=process.pid)

            # Wait for process to complete
            ret_code = process.wait()
            if ret_code == 0:
                update_status(name, 'completed')
            else:
                update_status(name, 'failed')
            # print("Simulating process for", name)
            # print("Using config file:", file_path)
        except Exception as e:
            update_status(name, 'failed')

    thread = threading.Thread(target=target)
    thread.start()



@app.route('/')
def index():
    return render_template('index.html')


@app.route('/list/key=<int:key_id>')
def parameter_page(key_id):
    return render_template('parameter_id.html', key_id=key_id)


@app.route('/register/<param_name>/key=<int:key_id>', methods=['GET', 'POST'])
def register_parameter(param_name, key_id):
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

    if request.method == 'POST':
        try:
            key = request.form.get('name')

            # Combine folder paths
            folder_path_main = request.form.get('folder_path_main') or ''
            folder_path_sub = request.form.get('folder_path_sub') or ''
            folder_path = os.path.join(folder_path_main, folder_path_sub).replace('\\', '/')

            replace_days_raw = request.form.getlist('replace_days_range[]')
            replace_days_range = [int(x) for x in replace_days_raw if x.strip() != '']

            param_config = {
                "param_name": key,
                "folder_path": folder_path,
                "categorical_data": request.form.get('categorical_data') == 'true',
                "exclude_values_for_stats": [],
                "update_flag": request.form.get('update_flag') == 'true',
                "param_execution_type": "process",
                "replace_days_range": replace_days_range
            }

            if param_config["categorical_data"]:
                # If categorical_data is true, exclude stats, include categorical_fn
                param_config["categorical_fn"] = request.form.get('categorical_fn')
            else:
                # If categorical_data is false, exclude categorical_fn, include stats
                param_config["stats"] = request.form.getlist('stats[]')

             # Update the param_template in the config
            if 'param_template' not in config_data['config']:
                config_data['config']['param_template'] = {}

            config_data['config']['param_template'][key] = [param_config]

            # ==== DYNAMIC ENTITY HANDLING ====
            entities = []
            entity_mapping = config_data['config']['mapping']['entity_mapping']
            mapping_keys_for_stats_gen = config_data['config']['mapping_keys_for_stats_gen']

            # Extract all form keys that start with 'entities['
            for form_key in request.form:
                if form_key.startswith("entities["):
                    import re
                    match = re.match(r'entities\[(\d+)\]\[(\w+)\]', form_key)
                    if match:
                        index, field = match.groups()
                        index = int(index)

                        while len(entities) <= index:
                            entities.append({})

                        if field == 'region_prefix_filter':
                            entities[index][field] = request.form.getlist(f'entities[{index}][{field}][]')
                        else:
                            entities[index][field] = request.form.get(form_key)

            for entity in entities:
                entity_name = entity.get('name')
                if not entity_name:
                    continue  # Skip incomplete

                entity_config = {
                    "source_id": int(entity.get('source_id')),
                    "region_prefix_filter": entity.get('region_prefix_filter', []),
                    "params": entity.get('params', ''),
                    "filter_by_file_name": entity.get('filter_by_file_name', 'false') == 'true',
                    "file_name_filter": entity.get('file_name_filter', '')
                }

                entity_mapping[entity_name] = {
                    **entity_mapping.get(entity_name, {}),  # keep old values if any
                    **entity_config                        # overwrite with new values
                }

                if entity_name not in mapping_keys_for_stats_gen:
                    mapping_keys_for_stats_gen.append(entity_name)

            # Save the updated config
            with open(config_path, 'w') as f:
                json.dump(config_data, f, indent=4)

            return redirect(url_for('parameter_page', key_id=key_id))

        except Exception as e:
            return f"Error during registration: {str(e)}", 500


    return render_template('register.html', param_name=param_name, key_id=key_id, config_data=config_data)


@app.route('/verify-path', methods=['POST'])
def verify_path():
    data = request.get_json()
    path = data.get('path')
    # print("Verifying path:", path)

    if not path:
        return jsonify({'error': 'No path provided'}), 400

    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(SSH_IP, username=SSH_USER, password=SSH_PASS)
        # print("SSH connection established")

        # Command to check if path exists
        cmd = f'test -d "{path}" && echo "exists" || echo "not exists"'

        # print("Executing command:", cmd)

        stdin, stdout, stderr = ssh.exec_command(cmd)
        result = stdout.read().decode().strip()
        ssh.close()
        # print("Command result:", result)

        exists = result == 'exists'

        # print(f"Path exists: {exists}")

        return jsonify({'exists': exists})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/configs', methods=['GET'])
def api_configs():
    configs = []
    try:
        for filename in os.listdir(CONFIG_DIR):
            if filename.endswith('.json'):
                param_name = os.path.splitext(filename)[0]
                configs.append({
                    'file_name': param_name,
                })
    except Exception as e:
        return jsonify({'error': str(e)}) , 500

    return jsonify({'data': configs})


@app.route('/existing-configs', methods=['GET'])
def existing_configs():
    return render_template('existing_configs.html')


@app.route('/api/run_process', methods=['POST'])
def run_process():
    data = request.json
    param_name = data.get('config_file')
    if not param_name:
        return jsonify({'error': 'No config_file provided'}), 400

    file_path = os.path.join(CONFIG_DIR, f'{param_name}.json')

    if not os.path.exists(file_path):
        return jsonify({'error': 'Config file not found'}), 404

    # Check if already running or completed
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT status FROM process_status WHERE param_name = ?', (param_name,))
    row = c.fetchone()
    conn.close()

    if row and row[0] == 'running':
        return jsonify({'status': 'running', 'message': 'Process already running'}), 400

    # Run process in background
    run_process_in_background(file_path, param_name)

    return jsonify({'status': 'started', 'message': f'Job has started for {param_name}'}), 200

@app.route('/api/status/<config_file>')
def get_status(config_file):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT status FROM process_status WHERE param_name = ?', (config_file,))
    row = c.fetchone()
    conn.close()

    status = row[0] if row else 'not_started'
    return jsonify({'status': status})


@app.route('/param-form', methods=['GET', 'POST'])
def param_form():
    return render_template('param_form.html')


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5004)
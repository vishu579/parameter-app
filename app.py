from flask import Flask, render_template, request, redirect, url_for, jsonify
import os
import json
import paramiko

app = Flask(__name__)

CONFIG_DIR = os.path.join(os.getcwd(), 'configs', 'geotifs')

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
                "categorical_fn": request.form.get('categorical_fn'),
                "stats": request.form.getlist('stats[]'),
                "exclude_values_for_stats": [],
                "update_flag": request.form.get('update_flag') == 'true',
                "param_execution_type": "process",
                "replace_days_range": replace_days_range
            }

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
                    "params": entity.get('params', '')
                }

                if entity_name not in entity_mapping:
                    entity_mapping[entity_name] = entity_config

                if entity_name not in mapping_keys_for_stats_gen:
                    mapping_keys_for_stats_gen.append(entity_name)

            # Save the updated config
            with open(config_path, 'w') as f:
                json.dump(config_data, f, indent=4)

            return redirect(url_for('parameter_page', key_id=key_id))

        except Exception as e:
            return f"Error during registration: {str(e)}", 500


    return render_template('register.html', param_name=param_name, key_id=key_id)


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


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5004)
from flask import Flask, jsonify
import psycopg2

app = Flask(__name__)

# Database configuration
DATABASE_CONFIG = {
    "dbname": "geoentity_stats",
    "user": "postgres",
    "password": "Vedas@123",
    "host": "192.168.2.149",
    "port": "5433"
}

@app.route("/parameters", methods=["GET"])
def parameters():
    try:
        conn = psycopg2.connect(**DATABASE_CONFIG)
        cur = conn.cursor()
        sql_query = """
            SELECT id, param_name, param_theme, param_gen_frequency, aggregation_period, param_displayname
                FROM public.parameters ;
        """
        cur.execute(sql_query)
        db_results = cur.fetchall()
        cur.close()
        conn.close()
        
        # Convert query results to list of dicts for JSON response
        keys = ["id", "param_name", "param_theme", "param_gen_frequency", "aggregation_period", "param_displayname"]
        data = [dict(zip(keys, row)) for row in db_results]

        response = {
            "count": len(data),
            "data": data
        }
        
        return jsonify(response)

    except Exception as e:
        print("Error is", e)

@app.route("/params-source-ids", methods=["GET"])
def parameters_source_ids():
    print("Request received")
    try:
        conn = psycopg2.connect(**DATABASE_CONFIG)
        print("Connected")
        cur = conn.cursor()
        sql_query = """
            SELECT DISTINCT
                p.id AS param_id,
                p.param_name,
                g.geoentity_source_id,
                p.param_theme
            FROM public.parameters p
            JOIN public.geoentity_param_time_stat g
                ON p.id = g.param_id;
        """
        
        print(sql_query)
        cur.execute(sql_query)
        print("Query executed")
        db_results = cur.fetchall()
        cur.close()
        conn.close()

        # Convert query results to list of dicts for JSON response
        keys = ["param_id", "param_name", "geoentity_source_id", "param_theme"]
        data = [dict(zip(keys, row)) for row in db_results]

        response = {
            "count": len(data),
            "data": data
        }

        return jsonify(response)

    except Exception as e:
        print("Error is", e)


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=5001)

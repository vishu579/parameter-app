from flask import Flask, render_template

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/list/key=<int:key_id>')
def parameter_page(key_id):
    return render_template('parameter_id.html', key_id=key_id)


@app.route('/register/<int:key_id>')
def register_parameter(key_id):
    return f"Register parameter with key_id: {key_id}"


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5004)
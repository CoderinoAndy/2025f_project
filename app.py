from flask import Flask 
app = Flask(__name__)

@app.route('/')
def index():
    return "Welcome to our flask web application!"

if __name__ == '__main__':
    app.run(debug=True)
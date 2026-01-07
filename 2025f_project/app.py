from flask import Flask, render_template 
app = Flask(__name__)

@app.route('/')
def index():
    return "Welcome to our flask web application!"

@app.route('/about')
def about():
    return(render_template("about.html"))

@app.route('/allemails')
def about():
    return(render_template("allemails.html"))

@app.route('/readonly')
def about():
    return(render_template("readonly.html"))

@app.route('/responseneeded')
def about():
    return(render_template("responseneeded.html"))

@app.route('/junkmailconfirm')
def about():
    return(render_template("junkmailconfirm.html"))

@app.route('/email')
def about():
    return(render_template("email.html"))

if __name__ == '__main__':
    app.run(debug=True)

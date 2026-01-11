from flask import Flask

def create_app():
    app = Flask(__name__, instance_relative_config=True)

    from .routes import main
    app.register_blueprint(main)

    return app
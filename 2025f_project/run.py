from app import create_app

app = create_app()
print(app.url_map) 

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5050, debug=True, use_reloader=False)
    

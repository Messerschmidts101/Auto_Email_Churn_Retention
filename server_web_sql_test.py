from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from server_database import db, Latest_Training

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///database.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# ✅ Initialize with app immediately
db.init_app(app)

# ✅ Create DB tables before first request
with app.app_context():
    db.create_all()

@app.route('/add_user', methods=['POST'])
def add_user():
    data = request.get_json()
    new_user = Latest_Training(name=data['name'], email=data['email'])
    db.session.add(new_user)
    db.session.commit()
    return jsonify({'message': 'User added'}), 201

@app.route('/users')
def get_users():
    users = Latest_Training.query.all()
    return jsonify([{'name': u.name, 'email': u.email} for u in users])

if __name__ == '__main__':
    app.run(debug=True)

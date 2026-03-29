from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import inspect, text
from app.db.server_database import db, Latest_Training, Latest_Scoring, Latest_Scored, Latest_Emails, Historical_Training, Historical_Scoring, Historical_Scored, Historical_Emails
import pandas as pd

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

@app.route('/tables')
def get_tables():
    inspector = inspect(db.engine)
    table_names = inspector.get_table_names()
    return jsonify([{'index':key,'name':val } for key,val in zip(range(len(table_names)),table_names)])


@app.route('/table_2')
def get_tables_2():
    strTableName = 'historical' + '__' + 'training'
    sql = text(f"SELECT * FROM \"{strTableName}\"")  # Quotes handle PascalCase or underscores
    with db.engine.connect() as conn:
        result = conn.execute(sql)
        df = pd.DataFrame(result.mappings().all())  # Convert to DataFrame
    
    df = df.drop(['meta_DateCreated', 'meta_Id'], axis=1, errors='ignore')  # Drop if present
    return jsonify(df.to_dict(orient="records"))

if __name__ == '__main__':
    app.run(debug=True)

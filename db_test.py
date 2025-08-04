from server_database import db, Historical_Training

# Query all rows
rows = db.session.query(Historical_Training).all()

# Print column names for the table
print(Historical_Training.__table__.columns.keys())

# Print the first row
print(rows[0].__dict__)
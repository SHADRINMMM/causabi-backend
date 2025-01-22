from . import db
from flask_login import UserMixin
from sqlalchemy.sql import func

class User(db.Model, UserMixin):
    __tablename__ = "user"
    name = db.Column(db.String(255))
    id = db.Column(db.Integer, primary_key=True)    
    email = db.Column(db.String(150), unique=True)
    current_file_name = db.Column(db.String,default = '')
    reg_dt = db.Column(db.DateTime(timezone=True), default=func.now())


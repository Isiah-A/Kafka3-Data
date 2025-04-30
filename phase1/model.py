
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


class Transaction(db.Model):
   id = db.Column(db.Integer, primary_key = True)
   custid = db.Column(db.Integer)
   type = db.Column(db.String(250), nullable = False)
   date = db.Column(db.Integer)
   amt = db.Column(db.Integer)

   def __repr__(self):
      return f"<Transaction {self.id}"

   def to_dict(self):
       return {
           'id': self.id,
           'custid': self.custid,
           'type': self.type,
           'date': self.date,
           'amt': self.amt
       }

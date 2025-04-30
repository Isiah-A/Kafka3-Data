from kafka import KafkaConsumer, TopicPartition
from json import loads
from flask import Flask
from model import db, Transaction
import numpy as np

class XactionConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer('bank-customer-events',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            value_deserializer=lambda m: loads(m.decode('ascii')))
        ## These are two python dictionarys
        # Ledger is the one where all the transaction get posted
        self.ledger = {}
        # custBalances is the one where the current blance of each customer
        # account is kept.
        self.custBalances = {}
        self.deposits = []
        self.withdrawl = []

        #update summary to see if its deposit or withdawl then append to either list
        #then make a print summary that tells you the mean
        # THE PROBLEM is every time we re-run the Consumer, ALL our customer
        # data gets lost!
        # add a way to connect to your database here.
        self.app = Flask(__name__)
        self.app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///database.db'
        self.app.config['SECRET_KEY'] = 'isiah'
        db.init_app(self.app)
        with self.app.app_context():
            db.create_all()

    def stat_summary(self, transaction):
        if transaction['type'] == 'dep':
            self.deposits.append(transaction['amt'])
        elif transaction['type'] == 'wth':
            self.withdrawl.append(transaction['amt'])
        self.print_summary()

    def print_summary(self):
        if self.deposits:
           mean_deposit = np.mean(self.deposits)
           std_deposit = np.std(self.deposits)
        else:
            mean_deposit = 0
            std_deposit = 0

        if self.withdrawl:
            mean_withdrawl = np.mean(self.withdrawl)
            std_withdrawl = np.std(self.withdrawl)
        else:
            mean_withdrawl = 0
            std_withdrawl = 0

        print("\n ----- Numerical Summary -----")
        print("Mean Deposit: ", mean_deposit)
        print("Standard Deviation Deposit: ", std_deposit)
        print("Mean Withdrawal: ", mean_withdrawl)
        print("Standard Deviation Withdrawal: ", std_withdrawl)
        print("-----------------------------")








    def handleMessages(self):
        with self.app.app_context():
            for message in self.consumer:
                message = message.value
                print('{} received'.format(message))
                transaction = Transaction(
                    custid = message['custid'],
                    type = message['type'],
                    date = message['date'],
                    amt = message['amt']
                )
                db.session.add(transaction)
                db.session.commit()
                self.stat_summary(message)
                print("Transaction added to database.")

if __name__ == "__main__":
    c = XactionConsumer()
    c.handleMessages()
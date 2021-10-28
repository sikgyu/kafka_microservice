from sqlalchemy import Column, Integer, String, DateTime
from base import Base
import datetime


class StudentAccount(Base):
    """ Student Account """

    __tablename__ = "student_account"

    id = Column(Integer, primary_key=True)
    fullname = Column(String(50), nullable=False)
    username = Column(String(100), nullable=False)
    password = Column(String(100), nullable=False)
    date_created = Column(DateTime, nullable=False)
    final_grade = Column(Integer, nullable=False)
    timestamp = Column(String(100), nullable=False)

    def __init__(self, fullname, username, password, final_grade, timestamp):
        """ Initializes a student account log """
        self.fullname = fullname
        self.username = username
        self.password = password
        self.date_created = datetime.datetime.now()  # Sets the date/time record is created
        self.final_grade = final_grade
        self.timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    def to_dict(self):
        """ Dictionary Representation of a student account log """
        dict = {}
        dict['id'] = self.id
        dict['fullname'] = self.fullname
        dict['username'] = self.username
        dict['final_grade'] = self.final_grade
        dict['password'] = self.password
        dict['date_created'] = self.date_created
        dict['timestamp'] = self.timestamp

        return dict

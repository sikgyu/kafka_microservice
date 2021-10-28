from sqlalchemy import Column, Integer, String, DateTime
from base import Base
import datetime


class InstructorAccount(Base):
    """  Instructor Account """

    __tablename__ = "instructor_account"

    id = Column(Integer, primary_key=True)
    fullname = Column(String(50), nullable=False)
    username = Column(String(100), nullable=False)
    password = Column(String(100), nullable=False)
    date_created = Column(DateTime, nullable=False)
    math = Column(String(50), nullable=False)
    python = Column(String(50), nullable=False)
    timestamp = Column(String(100), nullable=False)

    def __init__(self, fullname, username, password, math, python, timestamp):
        """ Initializes an instructor account log """
        self.fullname = fullname
        self.username = username
        self.password = password
        self.date_created = datetime.datetime.now()  # Sets the date/time record is created
        self.math = math
        self.python = python
        self.timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    def to_dict(self):
        """ Dictionary Representation of an instructor account log """
        dict = {}
        dict['id'] = self.id
        dict['fullname'] = self.fullname
        dict['username'] = self.username
        dict['password'] = self.password
        dict['classes'] = {}
        dict['classes']['math'] = self.math
        dict['classes']['python'] = self.python
        dict['date_created'] = self.date_created
        dict['timestamp'] = self.timestamp

        return dict

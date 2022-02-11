from sqlalchemy import Column, Integer, String, DateTime
from base import Base
import datetime


class PickupOrder(Base):
    """ Pickup order """

    __tablename__ = "pickup_order"

    id = Column(Integer, primary_key=True)
    total = Column(Integer, nullable=False)
    pickupPlace = Column(String(250), nullable=False)
    orderTime = Column(DateTime, nullable=False)
    cookReady = Column(Integer, nullable=False)
    items = Column(String(250), nullable=False)

    def __init__(self, total, items, cookReady, pickupPlace):
        """ Initializes a pickup order """
        self.total = total
        self.pickupPlace = pickupPlace
        # Sets the date/time record is created
        self.orderTime = datetime.datetime.now()
        self.cookReady = cookReady
        self.items = items

    def to_dict(self):
        """ Dictionary Representation of a blood pressure reading """
        dict = {}
        dict['id'] = self.id
        dict['total'] = self.total
        dict['items'] = self.items
        dict['pickupOrderInfo'] = {}
        dict['pickupOrderInfo']['cookReady'] = self.cookReady
        dict['pickupOrderInfo']['pickupPlace'] = self.pickupPlace
        dict['orderTime'] = self.orderTime

        return dict

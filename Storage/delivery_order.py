from sqlalchemy import Column, Integer, String, DateTime
from base import Base
import datetime


class DeliveryOrder(Base):
    """ Delivery order """

    __tablename__ = "delivery_order"

    id = Column(Integer, primary_key=True)
    total = Column(Integer, nullable=False)
    driverName = Column(String(250), nullable=False)
    orderTime = Column(DateTime, nullable=False)
    remainingTime = Column(Integer, nullable=False)
    address = Column(String(250), nullable=False)
    items = Column(String(250), nullable=False)

    def __init__(self, total, items, remainingTime, address, driverName):
        """ Initializes a delivery order """
        self.total = total
        self.items = items
        # Sets the date/time record is created
        self.orderTime = datetime.datetime.now()
        self.remainingTime = remainingTime
        self.address = address
        self.driverName = driverName

    def to_dict(self):
        """ Dictionary Representation of a delivery """
        dict = {}
        dict['total'] = self.total
        dict['items'] = self.items
        dict['deliveryOrderInfo'] = {}
        dict['deliveryOrderInfo']['remainingTime'] = self.remainingTime
        dict['deliveryOrderInfo']['address'] = self.address
        dict['deliveryOrderInfo']['driverName'] = self.driverName
        dict['orderTime'] = self.orderTime

        return dict

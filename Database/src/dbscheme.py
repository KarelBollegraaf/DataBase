from datetime import datetime
from sqlalchemy import create_engine, Engine, Enum, ForeignKey, Integer, String, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session
from typing import List
from enum import IntEnum

class Part(IntEnum):
    Ram = '1',
    Flap = '2',
    NeedlesVertical = '3',
    NeedlesHorizontal = '4',
    KnotterVertical = '5',
    KnotterHorizontal = '6',
    Knife = '7'

class Direction(IntEnum):
    Forward = '1',
    Reverse = '2',

class Base(DeclarativeBase):
    pass

class CycleTimes(Base):
    __tablename__ = "CycleTimes"

    id:                 Mapped[int]             = mapped_column(primary_key=True, unique=True)
    bale_id:            Mapped[int]             = mapped_column(ForeignKey("BaleData.id"))
    cycle_index:        Mapped[int]
    part:               Mapped[Part]            = mapped_column(Enum(Part))
    direction:          Mapped[Direction]       = mapped_column(Enum(Direction), nullable=True)
    time:               Mapped[int]

class ChannelPressure(Base):
    __tablename__ = "ChannelPressure"

    id:                 Mapped[int]             = mapped_column(primary_key=True, unique=True)
    bale_id:            Mapped[int]             = mapped_column(ForeignKey("BaleData.id"))
    stroke:             Mapped[int]
    part:               Mapped[Part]            = mapped_column(Enum(Part))
    direction:          Mapped[Direction]       = mapped_column(Enum(Direction))
    high_pressure:      Mapped[int]
    channel_pressure:   Mapped[int]

class BaleData(Base):
    __tablename__ = "BaleData"

    id:                 Mapped[int]             = mapped_column(primary_key=True, unique=True)
    bale_number:        Mapped[int]
    recipe_number:      Mapped[int]
    material_name:      Mapped[str]             = mapped_column(String(20))
    user_id:            Mapped[int]
    username:           Mapped[str]             = mapped_column(String(16))
    customer_number:    Mapped[int]
    shift_number:       Mapped[int]
    timestamp:          Mapped[datetime]    
    kwh_used:           Mapped[float]
    bale_length:        Mapped[int]
    wires_vertical:     Mapped[int]
    wires_horizontal:   Mapped[int]
    knots_vertical:     Mapped[int]
    knots_horizontal:   Mapped[int]
    weight:             Mapped[int]
    volume:             Mapped[int]
    oil_temperature:    Mapped[int]
    oil_level:          Mapped[int]
    total_time:         Mapped[int]
    auto_time:          Mapped[int]
    standby_time:       Mapped[int]
    empty_time:         Mapped[int]
    valve_lp:           Mapped[int]
    valve_hp:           Mapped[int]
    valve_ko1:          Mapped[int]
    valve_ko2:          Mapped[int]
    valve_kd1:          Mapped[int]
    valve_kd2:          Mapped[int]
    valve_rp1:          Mapped[int]
    valve_rp2:          Mapped[int]
    valve_rr1:          Mapped[int]
    valve_rr2:          Mapped[int]
    valve_ch:           Mapped[int]
    valve_mes:          Mapped[int]

# utility functions
def create_tables(engine: Engine) -> None:
    Base.metadata.create_all(engine)

def insert_entry(engine: Engine, bale_data: BaleData) -> int:
    with Session(engine) as session:
        session.add(bale_data)
        print("success")
        session.commit()
        return bale_data.id # increment

def insert_cycles(engine: Engine, cycle_times: List[CycleTimes]) -> None:
    with Session(engine) as session:
        for cycle in cycle_times:
            session.add(cycle)
        session.commit()

def insert_pressure_values(engine: Engine, pressure_values: List[ChannelPressure]) -> None:
    with Session(engine) as session:
        for value in pressure_values:
            session.add(value)
        session.commit()

def latest_entry(engine: Engine) -> BaleData:
    with Session(engine) as session:
        return session.query(BaleData).order_by(BaleData.timestamp.desc()).first()

def database_exists(db_uri: str) -> bool:
    uri = db_uri[:db_uri.rfind("/")]
    database = db_uri[db_uri.rfind("/") + 1:]
    
    engine = create_engine(uri)
    with engine.connect() as connection:
        return bool(connection.scalar(text(f"SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{database}'")))
    
def create_database(db_uri: str) -> None:
    uri = db_uri[:db_uri.rfind("/")]
    database = db_uri[db_uri.rfind("/") + 1:]

    engine = create_engine(uri, echo = True)
    with engine.connect() as connection:
        connection.execute(text(f"CREATE DATABASE {database}"))
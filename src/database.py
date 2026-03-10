from sqlalchemy import create_engine
import settings
import dbscheme

settings.init()

if not dbscheme.database_exists(settings.SQLALCHEMY_DATABASE_URI):
    print("does not exist, creating")
    dbscheme.create_database(settings.SQLALCHEMY_DATABASE_URI)

engine = create_engine(settings.SQLALCHEMY_DATABASE_URI, echo=False)
dbscheme.create_tables(engine)

previous_row = dbscheme.latest_entry(engine)

def parse_cycles(opcdata: list, bale_id: int) -> list[dbscheme.CycleTimes]:
    cycles = []
    for entry in opcdata:
        for idx in range(10):
            if (entry[2][idx] == 0):
                break

            cycles.append(dbscheme.CycleTimes(
                bale_id = bale_id,
                cycle_index = idx,
                part = entry[0],
                direction = entry[1],
                time = entry[2][idx]
            ))
    return cycles

def parse_pressure(data: list[dict], bale_id: int) -> list[dbscheme.ChannelPressure]:
    pressures = []
    for entry in data:
        high_pressure = entry["high_pressure"]
        channel_pressure = entry["channel_pressure"]

        for idx in range(10):
            if high_pressure[idx] == 0 and channel_pressure[idx] == 0:
                break

            pressures.append(dbscheme.ChannelPressure(
                bale_id = bale_id,
                stroke = idx,
                part = entry["part"],
                direction = entry["direction"],
                high_pressure = high_pressure[idx],
                channel_pressure = channel_pressure[idx]
            ))
    return pressures

def store_bale_data(fields: dict[str, any], cycles: list[any], pressure: list[any]):
    global previous_row
    if previous_row is None:
        print("No previous row, inserting new bale data")
        insert_id = dbscheme.insert_entry(engine, dbscheme.BaleData(**fields))
        dbscheme.insert_cycles(engine, parse_cycles(cycles, insert_id))  # bale_number was inser_id
        dbscheme.insert_pressure_values(engine, parse_pressure(pressure, insert_id))
    previous_row = dbscheme.latest_entry(engine) # this is a bit redundant but whatever
    if previous_row.bale_number == fields["bale_number"]:
        print(f"Already logged bale at {fields['bale_number']}, skipping...")
        return
    insert_id = dbscheme.insert_entry(engine, dbscheme.BaleData(**fields))
    dbscheme.insert_cycles(engine, parse_cycles(cycles, insert_id))  # bale_number was inser_id
    dbscheme.insert_pressure_values(engine, parse_pressure(pressure, insert_id))
    previous_row = dbscheme.latest_entry(engine) # this is a bit redundant but whatever
    
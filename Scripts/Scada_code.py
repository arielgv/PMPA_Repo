import pandas as pd
import os
from datetime import datetime

def scada_processing():
    #print("Hello World from scada_processing!")
    
    #  StationPoints.CSV
    df_station = process_station_points()
    if df_station is not None:
        generate_station_dat(df_station)
    
    #  AnalogPoints.csv
    df_analog = process_analog_points(df_station)
    if df_analog is not None:
        print("\nFirst rows of the analog points DataFrame:")
        print(df_analog.head(3))
        generate_analog_dat(df_analog)

    #  Statuspoints.csv
    df_status = process_status_points(df_station)
    if df_status is not None:
        print("\nFirst rows of the status points DataFrame:")
        print(df_status.head(3))
        generate_status_dat(df_status)

    # MeasurementUnits.CSV
    df_unit = process_measurement_units()
    if df_unit is not None:
        print("\nFirst rows of the measurement units DataFrame:")
        print(df_unit.head(3))
        generate_unit_dat(df_unit)

def process_station_points():
    csv_path = os.path.join('..', 'Inputs', 'StationPoints.CSV')
    
    if not os.path.exists(csv_path):
        print(f"Error: Could not find the file at {csv_path}")
        return None
    
    print(f"File found: {csv_path}")
    df_station = pd.read_csv(csv_path)
    
    if 'PKEY' not in df_station.columns:
        print("Error: The PKEY column is not present in StationPoints.CSV")
        return None
    
    column_mapping = {
        'NAME': 'Key',
        'DESC': 'Name',
        'ZONEID': 'pAORGroup',
        'ALRMPRIOR': 'Priority'
    }
    df_station = df_station.rename(columns=column_mapping)
    df_station['Order'] = range(1, len(df_station) + 1)
    
    print("\nFirst rows of the stations DataFrame:")
    print(df_station.head())
    
    return df_station

def process_analog_points(df_station):
    csv_path = os.path.join('..', 'Inputs', 'AnalogPoints.csv')
    if not os.path.exists(csv_path):
        print(f"Error: Could not find the file at {csv_path}")
        return None
    
    print(f"File found: {csv_path}")
    df = pd.read_csv(csv_path)
    
    df_analog = pd.DataFrame()
    df_analog['Type'] = df.get('RTUID', '').apply(lambda x: 2 if pd.isna(x) or x == '' else 1)
    df_analog['Name'] = df.get('NAME', '')
    df_analog['pUNIT'] = df.get('ENGUNITS', '')
    df_analog['pScale'] = df.get('SCALEFACT', '')
    df_analog['pConfiguredAORGroup'] = 1


    df_analog['NominalHiLimits1'] = df.get('PREMGHI', '')
    df_analog['NominalHiLimits2'] = df.get('PREMGSEVHI', '')
    df_analog['NominalHiLimits3'] = df.get('EMGHI', '')
    df_analog['NominalHiLimits4'] = df.get('EMGSEVHI', '')
    df_analog['NominalHiLimitsRnblty'] = 999999

    
    df_analog['NominalLowLimits1'] = df.get('PREMGLO', '')
    df_analog['NominalLowLimits2'] = df.get('PREMGSEVLO', '')
    df_analog['NominalLowLimits3'] = df.get('EMGLO', '')
    df_analog['NominalLowLimits4'] = df.get('EMGSEVLO', '')
    df_analog['NominalLowLimitsRnblty'] = -999999

    
    station_map = {key.upper(): order for key, order in zip(df_station['Key'], df_station['Order'])}
    
    
    df_analog['pStation'] = df['STATIONPID'].apply(lambda x: station_map.get(str(x).upper(), 0))
    
    
    def create_key(row, station_counter):
        xx = str(row['Type']).zfill(2)
        yyy = str(row['pStation']).zfill(3)
        zzz = str(station_counter[row['pStation']]).zfill(3)
        station_counter[row['pStation']] += 1
        return f"{xx}{yyy}{zzz}"

    station_counter = {station: 1 for station in df_analog['pStation'].unique()}
    df_analog['Key'] = df_analog.apply(lambda row: create_key(row, station_counter), axis=1)
    
    #print(df_analog.head())
    return df_analog

def process_status_points(df_station):
    csv_path = os.path.join('..', 'Inputs', 'StatusPoints.csv')
    if not os.path.exists(csv_path):
        print(f"Error: Could not find the file at {csv_path}")
        return None
    
    df_status = pd.read_csv(csv_path)
    
    prefix_suffixes_path = os.path.join('..', 'Inputs', 'PrefixSuffixes.csv')
    if not os.path.exists(prefix_suffixes_path):
        print(f"Error: Could not find the file at {prefix_suffixes_path}")
        return None
    
    df_prefix_suffixes = pd.read_csv(prefix_suffixes_path)
    
    # Create mappings
    prefix_suffixes_map = {name.upper(): int(pkey) for name, pkey in zip(df_prefix_suffixes['Name'], df_prefix_suffixes['PKey'])}
    station_map = {key.upper(): pkey for key, pkey in zip(df_station['Key'], df_station['PKEY'])}
    
    df_status_new = pd.DataFrame()
    df_status_new['Type'] = 1  # Set all Types to 1 for status points
    df_status_new['Name'] = df_status['NAME']
    df_status_new['pStation'] = df_status['STATIONPID'].apply(lambda x: station_map.get(str(x).upper(), 0))
    df_status_new['pStates'] = df_status['PREFSUFFID'].apply(lambda x: prefix_suffixes_map.get(x.upper(), 0) + 200 if x else 0)
    df_status_new['pALARM_GROUP'] = 1
    df_status_new['pConfiguredAORGroup'] = df_status['USERTYPEID']
    df_status_new['ConfigNormalState'] = df_status['NORMSTATE']
    
    
    integer_columns = ['Type', 'pStation', 'pStates', 'pALARM_GROUP', 'ConfigNormalState']
    default_values = {'Type': 1, 'pStation': 0, 'pStates': 0, 'pALARM_GROUP': 1, 'ConfigNormalState': 0}
    
    for col in integer_columns:
        df_status_new[col] = df_status_new[col].fillna(default_values[col]).astype('int32')
    
    # Create Key column
    def create_key(row, station_counter):
        xx = str(row['Type']).zfill(2)
        yyy = str(row['pStation']).zfill(3)
        zzz = str(station_counter[row['pStation']]).zfill(3)
        station_counter[row['pStation']] += 1
        return f"{xx}{yyy}{zzz}"

    station_counter = {station: 1 for station in df_status_new['pStation'].unique()}
    df_status_new['Key'] = df_status_new.apply(lambda row: create_key(row, station_counter), axis=1)
    
    return df_status_new

def process_measurement_units():
    csv_path = os.path.join('..', 'Inputs', 'MeasurementUnits.CSV')
    
    if not os.path.exists(csv_path):
        print(f"Error: Could not find the file at {csv_path}")
        return None
    
    print(f"File found: {csv_path}")
    df = pd.read_csv(csv_path)
    
    df_unit = pd.DataFrame()
    df_unit['record'] = range(1, len(df) + 1)
    df_unit['NAME'] = df['Desc']
    
    return df_unit

def generate_station_dat(df_station):
    dat_folder = os.path.join('..', 'Dat_files')
    if not os.path.exists(dat_folder):
        os.makedirs(dat_folder)
    
    station_filename = os.path.join(dat_folder, "station_dat.dat")
    
    now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    
    with open(station_filename, 'w') as f:
        f.write("*\n")
        f.write("*\n")
        f.write(f"* Creation Date/Time: {now}\n")
        f.write("*\n")
        f.write("*\tOrder\tKey\tName\tAOR\n")
        f.write("*\n")
        f.write("\t2\tSTATION\t0\t3\t4\t13\n")
        f.write("*\n")
        
        for _, row in df_station.iterrows():
            
            order = row.get('Order', '')
            key = row.get('Key', '')
            name = row.get('Name', '')
            paorgroup = row.get('pAORGroup', '')
            f.write(f"\t{order}\t{order}\t'{key}'\t'{name}'\t1\n")
        
        f.write(" 0")
    
    print(f".dat file generated: {station_filename}")


def generate_analog_dat(df_analog):
    dat_folder = os.path.join('..', 'Dat_files')
    if not os.path.exists(dat_folder):
        os.makedirs(dat_folder)
    
    analog_filename = os.path.join(dat_folder, "analog_dat.dat")
    
    now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    
    with open(analog_filename, 'w') as f:

        f.write("*\n")
        f.write("\t5\tANALOG\t0\t1\t2\t3\t4\t5\t6\t7\t23\t24\t64\t77:0\t77:1\t77:2\t77:3\t77:4\t78:0\t78:1\t78:2\t78:3\t78:4\n")
        f.write("*\trecord\torderNo\tType\tKey\tName\tpStation\tpUNIT\tpScale\tpConfiguredAORGroup\t"
                "NominalHiLimits77:0\tNominalHiLimits77:1\tNominalHiLimits77:2\tNominalHiLimits77:3\tNominalHiLimitsRnblty77:4\t"
                "NominalLowLimits78:0\tNominalLowLimits78:1\tNominalLowLimits78:2\tNominalLowLimits78:3\tNominalLowLimitsRnblty78:4\n")
        f.write("*\n")
        
        for index, row in df_analog.iterrows():
            record_order = index + 1
            f.write(f"\t{record_order}\t{record_order}\t{row['Type']}\t'{row['Key']}'\t'{row['Name']}'\t{row['pStation']}\t"
                    f"{row['pUNIT']}\t{row['pScale']}\t{row['pConfiguredAORGroup']}\t"
                    f"{row['NominalHiLimits1']}\t{row['NominalHiLimits2']}\t{row['NominalHiLimits3']}\t{row['NominalHiLimits4']}\t{row['NominalHiLimitsRnblty']}\t"
                    f"{row['NominalLowLimits1']}\t{row['NominalLowLimits2']}\t{row['NominalLowLimits3']}\t{row['NominalLowLimits4']}\t{row['NominalLowLimitsRnblty']}\n")
        
        f.write(" 0")
    
    print(f"analog_dat.dat file generated: {analog_filename}")


def generate_status_dat(df_status):
    dat_folder = os.path.join('..', 'Dat_files')
    if not os.path.exists(dat_folder):
        os.makedirs(dat_folder)

    status_filename = os.path.join(dat_folder, "status_dat.dat")

    with open(status_filename, 'w') as f:
        f.write("*\n")
        f.write("\t4\tSTATUS\t0\t1\t3\t4\t5\t19\t29\t40\t49\n")
        f.write("*\trecord\tOrderNo\tType\tKey\tName\tpStation\tpStates\tpALARM_GROUP\tpConfiguredAORGroup\tConfigNormalState\n")

        for index, row in df_status.iterrows():
            record = index + 1
            f.write(f"\t{record}\t{record}\t1\t'{row.get('Key', '')}'\t'{row.get('Name', '')}'\t{int(row.get('pStation', 0))}\t"
                    f"{row.get('pStates', '')}\t1\t1\t{row.get('ConfigNormalState', '')}\n")
        
        f.write(" 0")
    
    print(f"status_dat.dat file generated: {status_filename}")

def generate_unit_dat(df_unit):
    dat_folder = os.path.join('..', 'Dat_files')
    if not os.path.exists(dat_folder):
        os.makedirs(dat_folder)
    
    unit_filename = os.path.join(dat_folder, "unit_dat.dat")
    
    now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    
    with open(unit_filename, 'w') as f:
        f.write("*\n")

        f.write("*\n")
        f.write("\t2\tUNIT\t0\t1\n")
        f.write("*\trecord\tNAME\n")
        f.write("*\n")
        
        for _, row in df_unit.iterrows():
            f.write(f"\t{row.get('record', '')}\t'{row.get('NAME', '')}'\n")
        
        f.write(" 0")
    
    print(f"unit_dat.dat file generated: {unit_filename}")

if __name__ == "__main__":
    scada_processing()


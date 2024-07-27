import pandas as pd
import os
from datetime import datetime

def scada_processing():
    print("Hello World from scada_processing!")
    
    # Procesar StationPoints.CSV
    df_station = process_station_points()
    if df_station is not None:
        generate_station_dat(df_station)
    
    # Procesar AnalogPoints.csv
    df_analog = process_analog_points()
    if df_analog is not None:
        print("\nPrimeras filas del DataFrame de puntos analógicos:")
        print(df_analog.head())
        generate_analog_dat(df_analog)

    df_status = process_status_points(df_station)
    if df_status is not None:
        print("\nPrimeras filas del DataFrame de puntos de estado:")
        print(df_status.head())
        generate_status_dat(df_status)

def process_station_points():
    csv_path = os.path.join('..', 'Inputs', 'StationPoints.CSV')
    
    if not os.path.exists(csv_path):
        print(f"Error: No se pudo encontrar el archivo en {csv_path}")
        return None
    
    print(f"Archivo encontrado: {csv_path}")
    df_station = pd.read_csv(csv_path)
    
    # Asegurarse de que tenemos la columna PKEY
    if 'PKEY' not in df_station.columns:
        print("Error: La columna PKEY no está presente en StationPoints.CSV")
        return None
    
    column_mapping = {
        'NAME': 'Key',
        'DESC': 'Name',
        'ZONEID': 'pAORGroup',
        'ALRMPRIOR': 'Priority'
    }
    df_station = df_station.rename(columns=column_mapping)
    df_station['Order'] = range(1, len(df_station) + 1)
    
    print("\nPrimeras filas del DataFrame de estaciones:")
    print(df_station.head())
    
    return df_station

def process_analog_points():
    csv_path = os.path.join('..', 'Inputs', 'AnalogPoints.csv')
    
    if not os.path.exists(csv_path):
        print(f"Error: No se pudo encontrar el archivo en {csv_path}")
        return None
    
    print(f"Archivo encontrado: {csv_path}")
    df = pd.read_csv(csv_path)
    
    df_analog = pd.DataFrame()
    df_analog['Type'] = df.get('RTUID', '').apply(lambda x: 'C' if pd.isna(x) or x == '' else 'T')
    df_analog['Name'] = df.get('NAME', '')
    df_analog['pUNIT'] = df.get('ENGUNITS', '')
    df_analog['pScale'] = df.get('SCALEFACT', '')
    df_analog['pConfiguredAORGroup'] = df.get('USERTYPEID', '')
    df_analog['NominalHiLimits'] = df.apply(lambda row: "{},{},{},{}".format(
        row.get('PREMGHI', ''), row.get('EMGHI', ''), row.get('PREMDB', ''), row.get('EMGDB', '')), axis=1)
    df_analog['NominalHiLimits2'] = df.apply(lambda row: "{},{}".format(
        row.get('UNRSHI', ''), row.get('UNRSDB', '')), axis=1)
    df_analog['NominalLowLimits'] = df.apply(lambda row: "{},{},{},{}".format(
        row.get('PREMGLO', ''), row.get('EMGLO', ''), row.get('EMGDB', ''), row.get('PREMGDB', '')), axis=1)
    df_analog['NominalLowLimits2'] = df.apply(lambda row: "{},{}".format(
        row.get('UNRSLO', ''), row.get('UNRSDB', '')), axis=1)
    
    return df_analog

def process_status_points(df_station):
    csv_path = os.path.join('..', 'Inputs', 'StatusPoints.csv')
    
    if not os.path.exists(csv_path):
        print(f"Error: No se pudo encontrar el archivo en {csv_path}")
        return None
    
    print(f"Archivo encontrado: {csv_path}")
    df_status = pd.read_csv(csv_path)
    
    # Crear un diccionario de mapeo de NAME a PKEY desde df_station
    station_map = dict(zip(df_station['Key'], df_station['PKEY']))
    
    # Crear el nuevo DataFrame con las columnas especificadas
    df_status_new = pd.DataFrame()
    df_status_new['Type'] = 1
    df_status_new['Name'] = df_status['NAME']
    df_status_new['pStation'] = df_status['STATIONPID'].map(station_map)
    df_status_new['pStates'] = df_status['PREFSUFFID']
    df_status_new['pALARM_GROUP'] = 1
    df_status_new['pConfiguredAORGroup'] = df_status['USERTYPEID']
    df_status_new['ConfigNormalState'] = df_status['NORMSTATE']
    
    return df_status_new

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
        f.write("*\tOrder\tKey\tName\tpAORGroup\n")
        f.write("*\n")
        f.write("\t2\tSTATION\t0\t3\t4\t13\n")
        f.write("*\n")
        
        for _, row in df_station.iterrows():
            # Verificar si las columnas existen antes de acceder a ellas
            order = row.get('Order', '')
            key = row.get('Key', '')
            name = row.get('Name', '')
            paorgroup = row.get('pAORGroup', '')
            f.write(f"\t{order}\t{key}\t{name}\t{paorgroup}\n")
        
        f.write(" 0")
    
    print(f"Archivo .dat generado: {station_filename}")

def generate_analog_dat(df_analog):
    dat_folder = os.path.join('..', 'Dat_files')
    if not os.path.exists(dat_folder):
        os.makedirs(dat_folder)
    
    analog_filename = os.path.join(dat_folder, "analog_dat.dat")
    
    now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    
    with open(analog_filename, 'w') as f:
        f.write("*\n")
        f.write("\t5\tANALOG\t1\t4\t23\t24\t64\t77,1\t77,4\t78,1\t78,4\n")
        f.write("*\tType\tName\tpUNIT\tpScale\tpConfiguredAORGroup\tNominalHiLimits\tNominalHiLimits2\tNominalLowLimits\tNominalLowLimits2\n")

        f.write("*\n")
        
        for _, row in df_analog.iterrows():
            f.write(f"\t{row.get('Type', '')}\t{row.get('Name', '')}\t{row.get('pUNIT', '')}\t"
                    f"{row.get('pScale', '')}\t{row.get('pConfiguredAORGroup', '')}\t"
                    f"{row.get('NominalHiLimits', '')}\t{row.get('NominalHiLimits2', '')}\t"
                    f"{row.get('NominalLowLimits', '')}\t{row.get('NominalLowLimits2', '')}\n")
        
        f.write(" 0")
    
    print(f"Archivo analog_dat.dat generado: {analog_filename}")

def generate_status_dat(df_status):
    dat_folder = os.path.join('..', 'Dat_files')
    if not os.path.exists(dat_folder):
        os.makedirs(dat_folder)

    status_filename = os.path.join(dat_folder, "status_dat.dat")

    #now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    with open(status_filename, 'w') as f:
        f.write("*\n")
        f.write("\t4\tSTATUS\t1\t4\t5\t19\t29\t40\t49\n")
        f.write("*\tType\tName\tpStation\tpStates\tpALARM_GROUP\tpConfiguredAORGroup\tConfigNormalState\n")

        for _, row in df_status.iterrows():
            #f.write(f"\t{row.get('Type', '')}\t{row.get('Name', '')}\t{row.get('pStation', '')}\t"
            f.write(f"\t1\t'{row.get('Name', '')}'\t{row.get('pStation', '')}\t"
                    f"{row.get('pStates', '')}\t{row.get('pALARM_GROUP', '')}\t"
                    f"{row.get('pConfiguredAORGroup', '')}\t{row.get('ConfigNormalState', '')}\n")
        
        f.write(" 0")
    
    print(f"Archivo status_dat.dat generado: {status_filename}")

if __name__ == "__main__":
    scada_processing()


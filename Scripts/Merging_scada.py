import os
from datetime import datetime

def read_file_content(file_path):
    """Read and return the content of a file."""
    with open(file_path, 'r') as file:
        return file.read()

def compile_files(input_folder, file_names):
    """Compile content from multiple files."""
    compiled_content = []
    for file_name in file_names:
        file_path = os.path.join(input_folder, file_name)
        if not os.path.exists(file_path):
            print(f"Error: No se pudo encontrar el archivo en {file_path}")
            continue
        print(f"Archivo encontrado: {file_path}")
        content = read_file_content(file_path)
        compiled_content.append(content)
    return '\n'.join(compiled_content)

def merge_scada():
    # Define relative paths
    input_folder = os.path.join('..', 'Dat_files')
    output_folder = os.path.join('..', 'Populateables')
    
    # Ensure output folder exists
    os.makedirs(output_folder, exist_ok=True)

    # Define input files
    file_names = [
        'station_dat.dat',
        'status_dat.dat',
        'analog_dat.dat',
        'unit_dat.dat'
    ]

    # Compile content
    compiled_content = compile_files(input_folder, file_names)
    
    # If no content was compiled, exit the function
    if not compiled_content:
        print("No se pudo compilar ningún contenido. Verificar los archivos de entrada.")
        return

    # Add header and footer
    final_content = '10 SCADA.DB\n' + compiled_content + '\n0'

    # Write to output file
    current_date = datetime.now().strftime("%y%m%d")
    output_file = f'SCADA_{current_date}.dat'
    output_path = os.path.join(output_folder, output_file)
    
    with open(output_path, 'w') as final_file:
        final_file.write(final_content)
    
    print(f"Archivo {output_file} generado correctamente en {output_path}")

if __name__ == "__main__":
    merge_scada()
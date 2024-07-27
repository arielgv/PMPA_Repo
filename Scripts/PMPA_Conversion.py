import pandas as pd
import time
from datetime import datetime
from Scada_code import scada_processing
from Merging_scada import merge_scada

def main():
    start_time = time.time()
    print(f"Starting {datetime.now().strftime('%H:%M:%S')}")


    print("Calling scada_processing...")
    scada_processing()


    # fep_processing()
    # iccp_processing()
    print("Calling merge_scada...")
    merge_scada()
    # merge_fep()

    end_time = time.time()
    print(f"Finished. Total time: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()
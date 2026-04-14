import sys
import os
import openzen
import pandas as pd
import threading
import time

# Initialize OpenZen
openzen.set_log_level(openzen.ZenLogLevel.Warning)
error, client = openzen.make_client()
if not error == openzen.ZenError.NoError:
    print("Error while initializing OpenZen library")
    sys.exit(1)

# Global Variables
foundlist = []
conections = {}
imus = {}
1
dataSensor1 = []
dataSensor2 = []
dataSensor3 = []

persona = ''
tanda = ''
aligned_tanda_dfs = []  # Master list to hold all 5 processed tiros

# --- Sensor Setup & Connection Functions ---

def findsensors():
    print("Finding sensors...")
    foundlist.clear() 
    client.list_sensors_async() 
    
    while True:
        zenEvent = client.wait_for_next_event()

        if zenEvent.event_type == openzen.ZenEventType.SensorFound:
            print("Found sensor {} on IoType {}".format(zenEvent.data.sensor_found.name,
                zenEvent.data.sensor_found.io_type))
            foundlist.append(zenEvent.data.sensor_found)

        if zenEvent.event_type == openzen.ZenEventType.SensorListingProgress:
            lst_data = zenEvent.data.sensor_listing_progress
            if lst_data.progress == 1.0:
                print("Sensor listing complete.")
                break

def connect_sensors():
    if len(foundlist) == 0:
        print("Please find sensors first (Option 1).")
        return
        
    for sensor_desc in foundlist:
        try:
            print("Connecting to {}...".format(sensor_desc.name))
            error, sensor = client.obtain_sensor(sensor_desc)
            if error == openzen.ZenError.NoError:
                conections[sensor_desc.name] = sensor
                print("Connected to {}".format(sensor_desc.name))
            else:
                print("Failed to connect to {}".format(sensor_desc.name))
        except Exception as e:
            print("Exception connecting to {}: {}".format(sensor_desc.name, e))

def findimus():
    if len(conections) == 0:
        print("Please connect to sensors first (Option 2).")
        return
        
    for name, sensor in conections.items():
        imu = sensor.get_any_component_of_type(openzen.component_type_imu)
        
        if imu is None:
            print("Error finding IMU for {}".format(name))
        else:
            print("Optimizing sensor {} for archery (200Hz)...".format(name))
            
            imu.set_int32_property(openzen.ZenImuProperty.SamplingRate, 200)
            imu.set_int32_property(openzen.ZenImuProperty.AccRange, 16)
            
            # Disable unused outputs for bandwidth, but KEEP QUATERNIONS ON for LinAcc!
            imu.set_bool_property(openzen.ZenImuProperty.OutputEuler, False)
            imu.set_bool_property(openzen.ZenImuProperty.OutputQuat, True) 
            imu.set_bool_property(openzen.ZenImuProperty.OutputRawMag, True)
            imu.set_bool_property(openzen.ZenImuProperty.OutputTemperature, False)
            
            # Enable Linear Acceleration
            imu.set_bool_property(openzen.ZenImuProperty.OutputLinearAcc, True)
            
            # Pause stream so it doesn't flood the PC
            imu.set_bool_property(openzen.ZenImuProperty.StreamData, False)
            
            imus[name] = imu
            print("Sensor {} found, optimized, and paused!".format(name))

def check_streaming():
    if len(imus) == 0:
        print("No IMUs found.")
        return
    for name, imu in imus.items():
        error, is_streaming = imu.get_bool_property(openzen.ZenImuProperty.StreamData)
        if error == openzen.ZenError.NoError:
            print("Sensor {} streaming status: {}".format(name, is_streaming))
        else:
            print("Error checking stream status for {}".format(name))

# --- Session Setup Functions ---

def setup_persona():
    global persona
    persona = input("Nombre de persona: ")
    print("Datos para: {}".format(persona))

def setup_tanda():
    global tanda
    tanda = input("Numero de tanda: ")
    print("Datos para tanda: {}".format(tanda))

# --- Bulletproof High-Speed Data Streaming ---

def stream_data():
    if len(imus) == 0:
        print("Error: No IMUs connected. Please run steps 1-3 first.")
        return

    print("\nWaking up sensors and waiting 1.5s for Bluetooth to catch up...")
    for imu in imus.values():
        try:
            imu.set_bool_property(openzen.ZenImuProperty.StreamData, True)
        except Exception:
            print("Warning: Could not wake up a sensor. It may be disconnected.")

    # FIX 1: Wait for delayed packets, flush the buffer, THEN clear arrays
    time.sleep(1.5)
    while client.poll_next_event() is not None:
        pass 

    clear_data() 

    print("Starting data stream... Press Ctrl+C to stop recording.")
    streaming_active = True
    
    def update_console():
        while streaming_active:
            print("\rLive Data Count: [Sensor 1: {}] [Sensor 2: {}] [Sensor 3: {}]   ".format(
                len(dataSensor1), len(dataSensor2), len(dataSensor3)), end="")
            time.sleep(0.1) 
            
    ui_thread = threading.Thread(target=update_console, daemon=True)
    ui_thread.start()
    
    # Build High-Speed Tuple Router
    target_lists = {}
    if len(foundlist) > 0: target_lists[foundlist[0].name] = dataSensor1
    if len(foundlist) > 1: target_lists[foundlist[1].name] = dataSensor2
    if len(foundlist) > 2: target_lists[foundlist[2].name] = dataSensor3
    
    fast_routers = []
    for imu_name, imu_obj in imus.items():
        if imu_name in target_lists:
            fast_routers.append( (imu_obj.sensor, imu_obj.component.handle, target_lists[imu_name]) )

    try:
        while True:
            zenEvent = client.wait_for_next_event()
            
            if zenEvent.event_type == openzen.ZenEventType.SensorDisconnected:
                print("\n\n[!] WARNING: A sensor physically disconnected!")
                break 
                
            if zenEvent.event_type == openzen.ZenEventType.ImuData: 
                for s_ref, c_handle, target_array in fast_routers:
                    if zenEvent.sensor == s_ref and zenEvent.component.handle == c_handle:
                        target_array.append(zenEvent.data.imu_data)
                        break
                        
    except KeyboardInterrupt:
        pass 
    except Exception as e:
        print("\n\n[!] Unexpected Error during streaming: {}".format(e))
        print("Forcing safe shutdown to preserve collected data...")
        
    finally:
        streaming_active = False 
        ui_thread.join(timeout=0.5)
        
        print("\nStopping recording... Pausing active sensors...")
        for imu_name, imu in imus.items():
            try:
                imu.set_bool_property(openzen.ZenImuProperty.StreamData, False)
            except Exception:
                pass
            
        print("Draining remaining data packets from the buffer...")
        try:
            while True:
                zenEvent = client.poll_next_event()
                if zenEvent is None:
                    break 
                if zenEvent.event_type == openzen.ZenEventType.ImuData:
                    for s_ref, c_handle, target_array in fast_routers:
                        if zenEvent.sensor == s_ref and zenEvent.component.handle == c_handle:
                            target_array.append(zenEvent.data.imu_data)
                            break
        except Exception:
            pass 

        print("Recording safely stopped! Data preserved.")

# --- Automated Tanda Control ---

def measure_tanda():
    global aligned_tanda_dfs
    if not persona or not tanda:
        print("Error: Please setup persona (Option 5) and tanda (Option 6) first.")
        return
    aligned_tanda_dfs.clear() 
    print("\n" + "="*45)
    print("  STARTING TANDA {} FOR {} (5 TIROS)".format(tanda, persona.upper()))
    print("="*45)

    for current_tiro in range(1, 6):
        input("\n>>> Press Enter when ready to start recording TIRO {}/5...".format(current_tiro))

        stream_data() 

        print("Processing Tiro {}...".format(current_tiro))
        all_data = []

        # FIX 2: Manually calculate LinAcc using 3D Quaternions
        def extract_data(sensor_data_list, sensor_id):
            for data in sensor_data_list:
                # Grab Quaternions (w, x, y, z)
                qw = data.q[0]
                qx = data.q[1]
                qy = data.q[2]
                qz = data.q[3]
                
                # Calculate the gravity vector
                gx = 2.0 * (qw * qy - qx * qz)
                gy = -2.0 * (qw * qx + qy * qz)
                gz = 2.0 * (qx * qx + qy * qy) - 1.0
                
                # Subtract gravity from raw acceleration
                lin_x = data.a[0] - gx
                lin_y = data.a[1] - gy
                lin_z = data.a[2] - gz
                    
                all_data.append({
                    'Tiro': current_tiro,
                    'SensorId': sensor_id,
                    'TimeStamp (s)': data.timestamp,
                    'AccX (g)': data.a[0],
                    'AccY (g)': data.a[1],
                    'AccZ (g)': data.a[2],
                    'GyroX (deg/s)': data.g1[0],
                    'GyroY (deg/s)': data.g1[1],
                    'GyroZ (deg/s)': data.g1[2],
                    'LinAccX (g)': lin_x,
                    'LinAccY (g)': lin_y,
                    'LinAccZ (g)': lin_z
                })

        # --- FIX 2: Explicitly rename the hardware MACs to "Sensor 1, 2, 3" ---
        if len(foundlist) > 0: extract_data(dataSensor1, foundlist[0].name)
        if len(foundlist) > 1: extract_data(dataSensor2, foundlist[1].name)
        if len(foundlist) > 2: extract_data(dataSensor3, foundlist[2].name)

        if len(all_data) > 0:
            df = pd.DataFrame(all_data)
            
            aligned_df = df.pivot_table(index=['Tiro', 'TimeStamp (s)'], columns='SensorId', aggfunc='mean')
            aligned_df = aligned_df.interpolate(method='linear').dropna()
            
            long_df = aligned_df.stack(level='SensorId', future_stack=True).reset_index()
            
            # --- FIX 3: Reset the timeline to exactly 0.0s AFTER dropping the messy head-start ---
            min_timestamp = long_df['TimeStamp (s)'].min()
            long_df['TimeStamp (s)'] = long_df['TimeStamp (s)'] - min_timestamp
            
            long_df['FrameNumber'] = long_df.groupby(['Tiro', 'SensorId']).cumcount() + 1
            
            final_cols = ['Tiro', 'SensorId', 'TimeStamp (s)', 'FrameNumber', 
                        'AccX (g)', 'AccY (g)', 'AccZ (g)', 
                        'GyroX (deg/s)', 'GyroY (deg/s)', 'GyroZ (deg/s)', 
                        'LinAccX (g)', 'LinAccY (g)', 'LinAccZ (g)']
                        
            long_df = long_df[final_cols]
            
            aligned_tanda_dfs.append(long_df)
            print("Tiro {} processed successfully! ({} synchronized rows)".format(current_tiro, len(long_df)))
        else:
            print("Warning: No data recorded for Tiro {}.".format(current_tiro))

    clear_data() 

    print("\n" + "="*45)
    print("  TANDA {} COMPLETE!".format(tanda))
    print("="*45)
    
    save_tanda_data()
    
# --- Saving & Cleanup Functions ---

def clear_data():
    dataSensor1.clear()
    dataSensor2.clear()
    dataSensor3.clear()

def checkdatasize():
    print("Data points currently in memory:")
    print("Sensor 1: {}".format(len(dataSensor1)))
    print("Sensor 2: {}".format(len(dataSensor2)))
    print("Sensor 3: {}".format(len(dataSensor3)))

def save_tanda_data():
    if len(aligned_tanda_dfs) == 0:
        print("Error: No tanda data to save.")
        return

    base_filename = 'data_{}_tanda_{}'.format(persona, tanda)
    filename = base_filename + '.csv'
    
    counter = 2
    while os.path.exists(filename):
        filename = '{}_v{}.csv'.format(base_filename, counter)
        counter += 1
        
    print("\nPreparing to save master Tanda file as {}...".format(filename))
    
    final_df = pd.concat(aligned_tanda_dfs, ignore_index=True)

    print("\n" + "="*30)
    print("      FINAL TANDA PREVIEW")
    print("="*30)
    print("Total Tiros collected:   {}".format(final_df['Tiro'].nunique()))
    print("Total SYNCHRONIZED rows: {}".format(len(final_df)))
    print("="*30 + "\n")
    
    confirm = input("Does this look correct? Save to {}? (y/n): ".format(filename))
    
    if confirm.lower() == 'y':
        final_df.to_csv(filename, index=False) 
        print("Successfully saved {} rows of data to {}!".format(len(final_df), filename))
        aligned_tanda_dfs.clear() 
    else:
        print("Save cancelled. Your data is still safely in memory.")

def close_connections():
    print("Closing connections...")
    conections.clear()
    imus.clear()
    foundlist.clear()
    print("Connections closed and memory cleared.")

# --- Main Menu Loop ---

def menu():
    print("\n--- OpenZen Archery Sensor Menu ---")
    print("1. Find sensors")
    print("2. Connect to sensors")
    print("3. Find & Optimize IMUs (200Hz)")
    print("4. Check if streaming")
    print("-" * 35)
    print("5. Setup Persona (Subject Name)")
    print("6. Setup Tanda (Batch Number)")
    print("7. Measure FULL Tanda (Auto-record 5 Tiros)")
    print("-" * 35)
    print("8. Check current array sizes")
    print("9. Save Tanda Data manually")
    print("10. Clear data memory manually")
    print("11. Close connections")
    print("12. Exit")
    
    choice = input("\nEnter your choice: ")
    
    match choice:
        case "1":
            findsensors()
        case "2":
            connect_sensors()
        case "3":
            findimus()
        case "4":
            check_streaming()
        case "5":
            setup_persona()
        case "6":
            setup_tanda()
        case "7":
            measure_tanda()
        case "8":
            checkdatasize()
        case "9":
            save_tanda_data()
        case "10":
            clear_data()
            print("Sensor memory manually cleared.")
        case "11":
            close_connections()
        case "12":
            print("Exiting program...")
            if len(conections) > 0:
                close_connections()
            client.close()
            return False
        case _:
            print("Invalid choice. Please try again.")
            
    return True

def main():
    keep_running = True
    while keep_running:
        keep_running = menu()

if __name__ == "__main__":
    main()
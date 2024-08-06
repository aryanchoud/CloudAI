import subprocess
import threading
import redis

def monitor_usage():
    #Use bwm-ng to monitor network and disk throughput, storing results in Redis.
    
    
    # Start bwm-ng to output network and disk data in CSV format
    process = subprocess.Popen(['bwm-ng', '-o', 'csv', '-c', '1', '-T', 'rate'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # Connect to Redis
    r = redis.Redis(host='redis-server', port=6379, db=0)

    try:
        # Read bwm-ng output line by line
        for line in process.stdout:
            data = line.decode().strip().split(';')
            disk_network_data = parse_data(data)
            # Store data in Redis using the interface name as a key
            if disk_network_data:
                r.hset(f"pod_usage_data:{disk_network_data['interface']}", mapping=disk_network_data)
    except Exception as e:
        print(f"Error processing data: {e}")
    finally:
        process.terminate()

def parse_data(data):
    """
    Parse CSV data from bwm-ng into a dict format.
    """
    # Ensure data contains enough columns
    if not data or len(data) < 4:
        return None
    # Convert string data to appropriate types and return as dict
    return {
        "interface": data[0],
        "outgoing_bytes_per_second": int(data[1]),
        "incoming_bytes_per_second": int(data[2]),
        "total_bytes_per_second": int(data[3])
    }

if __name__ == '__main__':
    threading.Thread(target=monitor_usage).start()

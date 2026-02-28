import requests
import time
import uuid

# VM instance IP address
US_IP = "34.42.78.11"
EU_IP = "34.77.45.89"

def get_avg_latency(url, mode="GET", data=None):
    latencies = []
    for _ in range(10):
        start = time.time()
        if mode == "GET":
            requests.get(url)
        else:
            requests.post(url, json=data)
        latencies.append((time.time() - start) * 1000)
    return sum(latencies) / 10

print("--- Part A: Latency Measurement ---")
# For us center 1
us_reg_lt = get_avg_latency(f"http://{US_IP}:8080/register", "POST", {"username": "test"})
us_list_lt = get_avg_latency(f"http://{US_IP}:8080/list")
# For eu center 1
eu_reg_lt = get_avg_latency(f"http://{EU_IP}:8080/register", "POST", {"username": "test"})
eu_list_lt = get_avg_latency(f"http://{EU_IP}:8080/list")

print(f"US Instance - Avg /register: {us_reg_lt:.2f}ms, /list: {us_list_lt:.2f}ms")
print(f"EU Instance - Avg /register: {eu_reg_lt:.2f}ms, /list: {eu_list_lt:.2f}ms")

print("\n--- Part B: Eventual Consistency Test (100 rounds) ---")
miss_count = 0
for i in range(100):
    new_user = f"user_{uuid.uuid4().hex[:6]}"
    # 1. Register in the US
    requests.post(f"http://{US_IP}:8080/register", json={"username": new_user})
    # 2. Check in the EU
    response = requests.get(f"http://{EU_IP}:8080/list").json()
    # 3. Check if exists
    if new_user not in response.get('users', []):
        miss_count += 1
    if (i+1) % 10 == 0: print(f"Progress: {i+1}/100...")

print(f"\nFinal Results for Analysis.txt:")
print(f"Average Latencies recorded.")
print(f"Total Synchronization Misses: {miss_count} out of 100")
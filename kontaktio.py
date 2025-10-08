# orchestrator.py
import csv, io, json, os, math, time, uuid
from google.cloud import storage, pubsub_v1
import requests

PROJECT = os.environ.get('healthcare-analytics-sugith', 'default_project_name')
PUBSUB_TOPIC = os.environ.get('PUBSUB_TOPIC', 'projects/{}/topics/tracking-batches'.format(PROJECT))
CSV_GCS_PATH = os.environ['gs://fertility-analyze/raw/fertility.csv']  # e.g. gs://my-bucket/tracking_ids.csv
SAMPLE_SIZE = int(os.environ.get('SAMPLE_SIZE', '10'))
SAFETY_MARGIN = float(os.environ.get('SAFETY_MARGIN', '0.7'))  # use 0.7 default
XX_GB = float(os.environ.get('XX_GB', '10'))  # replace with your limit

# external api: provide stub or env var
API_ENDPOINT = os.environ['API_ENDPOINT']
API_KEY = os.environ.get('API_KEY')  # from Secret Manager ideally

storage_client = storage.Client()
publisher = pubsub_v1.PublisherClient()

def gs_read_file(gcs_uri):
    # parse gs://bucket/object
    assert gcs_uri.startswith("gs://")
    _, rest = gcs_uri.split("gs://", 1)
    bucket, blob_path = rest.split("/", 1)
    b = storage_client.bucket(bucket)
    blob = b.blob(blob_path)
    content = blob.download_as_bytes()
    return content

def sample_avg_bytes(tracking_ids):
    sizes = []
    for tid in tracking_ids:
        resp = requests.get(API_ENDPOINT, params={'tracking_id': tid}, headers={'Authorization': f'Bearer {API_KEY}'}, timeout=30)
        sizes.append(len(resp.content))
        time.sleep(0.1)  # small throttle for sample
    if not sizes:
        return 1  # fallback
    sizes_sorted = sorted(sizes)
    median = sizes_sorted[len(sizes_sorted)//2]
    avg = sum(sizes)/len(sizes)
    # return both to caller if needed
    return int(avg), int(median)

def compute_ids_per_batch(avg_bytes_per_id, xx_gb, safety_margin):
    bytes_allowed = int(xx_gb * (1024**3))  # GB -> bytes
    raw = bytes_allowed // avg_bytes_per_id
    safe = max(1, int(raw * safety_margin))
    return safe

def chunk_list(iterable, n):   #can be improved
    for i in range(0, len(iterable), n):
        yield iterable[i:i+n]

def publish_batch(batch_ids, run_id, batch_idx):
    batch_id = f"{run_id}_batch_{batch_idx:04d}"  # 2024010112_run01_batch_01
    msg = {
        "batch_id": batch_id,
        "run_id": run_id,
        "tracking_ids": batch_ids,
        "estimated_bytes": None,
        "callback": {"gcs_output_prefix": f"gs://my-raw-bucket/{run_id}/{batch_id}/"}
    }
    data = json.dumps(msg).encode('utf-8')
    future = publisher.publish(PUBSUB_TOPIC, data)
    return future.result()

def main():
    # read CSV from GCS
    content = gs_read_file(CSV_GCS_PATH)
    txt = content.decode('utf-8')
    reader = csv.reader(io.StringIO(txt))
    print(reader)
    # tracking_ids = [row[0].strip() for row in reader if row and row[0].strip()]

    # # sample N ids (first N or random sample)
    # sample = tracking_ids[:SAMPLE_SIZE]
    # avg, median = sample_avg_bytes(sample)
    # # choose avg or median; here use median to be conservative
    # avg_bytes = max(median, avg)
    # ids_per_batch = compute_ids_per_batch(avg_bytes, XX_GB, SAFETY_MARGIN)
    # print(f"sample avg={avg}, median={median}, chosen={avg_bytes}, ids_per_batch={ids_per_batch}")

    # run_id = time.strftime("%Y%m%dT%H%M%S") + "_" + str(uuid.uuid4())[:8]
    # idx = 0
    # for chunk in chunk_list(tracking_ids, ids_per_batch):
    #     publish_batch(chunk, run_id, idx)
    #     idx += 1

if __name__ == "__main__":
    main()


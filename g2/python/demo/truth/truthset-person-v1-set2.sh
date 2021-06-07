
G2Loader.py \
    --fileSpec truthset-person-v1-set2-data.csv/?data_source=TRUTH-SET2

G2Snapshot.py \
    --output_file_root truthset-person-v1-set2-snapshot \
    --export_csv

G2Audit.py \
    --newer_csv_file truthset-person-v1-set2-snapshot.csv \
    --prior_csv_file truthset-person-v1-set2-key.csv \
    --output_file_root truthset-person-v1-set2-audit

G2Explorer.py \
    --snapshot_json_file truthset-person-v1-set2-snapshot.json \
    --audit_json_file truthset-person-v1-set2-audit.json


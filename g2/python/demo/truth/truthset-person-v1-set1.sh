
G2Loader.py \
    --purgeFirst \
    --fileSpec truthset-person-v1-set1-data.csv/?data_source=TRUTH-SET1

G2Snapshot.py \
    --output_file_root truthset-person-v1-set1-snapshot

G2Audit.py \
    --newer_csv_file truthset-person-v1-set1-snapshot.csv \
    --prior_csv_file truthset-person-v1-set1-key.csv \
    --output_file_root truthset-person-v1-set1-audit

G2Explorer.py \
    --snapshot_json_file truthset-person-v1-set1-snapshot.json \
    --audit_json_file truthset-person-v1-set1-audit.json

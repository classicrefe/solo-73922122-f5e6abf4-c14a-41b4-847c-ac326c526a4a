#!/usr/bin/env bash
# Demonstration against the real docker-compose stack (db + two API instances
# behind nginx on :8080).
#
#   bash scripts/demo_compose.sh
#
# Shows:
#   1. strict intake + idempotent retry / conflicting-content rejection;
#   2. the drift rule (first three released, then a >5nm drift is quarantined,
#      quarantined batches never enter the reference window);
#   3. slow head-of-line batch: same-device FIFO, other devices unaffected;
#   4. container crash: `docker stop`-like failure mid-batch, survivor takes
#      over with no half batch and no double reference advancement;
#   5. the data endpoint serves released batches only;
#   6. sealed snapshots: frozen pending status, released detail + references,
#      identical retry / conflicting parameters, unaccepted range rejection.
set -euo pipefail

BASE="${BASE:-http://localhost:8080}"
DEV="EQ-DEMO-$$"
j() { python3 -c "import sys,json;d=json.load(sys.stdin);print(json.dumps(d,ensure_ascii=False,indent=2))"; }

csv() { # $1=prefix $2=thickness $3=count
  { echo "wafer_no,thickness_nm"
    for i in $(seq 1 "$3"); do printf "%s-W%03d,%s\n" "$1" "$i" "$2"; done; }
}

post() { # $1=batch $2=device $3=file
  curl -sS -w '\n%{http_code}\n' -X POST "$BASE/batches" \
    -F "batch_no=$1" -F "device_id=$2" -F "file=@$3;type=text/csv"
}
status_of() { curl -sS "$BASE/batches/$1" | python3 -c 'import sys,json;print(json.load(sys.stdin)["status"])'; }
wait_decided() { # $1=batch
  for _ in $(seq 1 100); do
    s=$(status_of "$1")
    [ "$s" != "PENDING" ] && { echo "$s"; return; }
    sleep 0.3
  done
  echo "TIMEOUT"; exit 1
}

echo "== 0. health =="
curl -sS "$BASE/health" | j

echo; echo "== 1. intake: first three baselines on $DEV =="
for i in 1 2 3; do
  csv "D$i" 500 5 > "/tmp/demo_d$i.csv"
  post "DEMO-B$i" "$DEV" "/tmp/demo_d$i.csv" | tail -1
done
for i in 1 2 3; do echo "DEMO-B$i -> $(wait_decided DEMO-B$i)"; done

echo; echo "== 2. drift: +6nm quarantined, exactly +5nm released =="
csv DQ 506 5 > /tmp/demo_q.csv
post DEMO-Q "$DEV" /tmp/demo_q.csv | tail -1
echo "DEMO-Q -> $(wait_decided DEMO-Q)"
csv DE 505 5 > /tmp/demo_e.csv
post DEMO-E "$DEV" /tmp/demo_e.csv | tail -1
echo "DEMO-E -> $(wait_decided DEMO-E)"
curl -sS "$BASE/batches/DEMO-Q" | j

echo; echo "== 3. idempotent retry reused; different content rejected (409) =="
post DEMO-B1 "$DEV" /tmp/demo_d1.csv | j | grep -E 'reused_retry|status'
csv DIFF 501 5 > /tmp/demo_diff.csv
echo "conflict http status: $(post DEMO-B1 "$DEV" /tmp/demo_diff.csv | tail -1)"

echo; echo "== 4. slow head-of-line batch on $DEV-SLOW (other device flows) =="
SLOWDEV="${DEV}-SLOW"; OTHERDEV="${DEV}-OTHER"
csv S1 500 5 > /tmp/s1.csv; csv S2 500 5 > /tmp/s2.csv
csv O1 700 5 > /tmp/o1.csv; csv O2 700 5 > /tmp/o2.csv
S1ID=$(post SLOW-1 "$SLOWDEV" /tmp/s1.csv | sed -n '1p' | python3 -c 'import sys,json;print(json.load(sys.stdin)["batch_id"])')
post SLOW-2 "$SLOWDEV" /tmp/s2.csv >/dev/null
# Mark the head batch slow inside the shared raw volume (both containers see it).
docker compose exec -T api1 sh -c "echo '$S1ID:8' > /data/raw/slow_batches.txt"
post OTHER-1 "$OTHERDEV" /tmp/o1.csv >/dev/null
post OTHER-2 "$OTHERDEV" /tmp/o2.csv >/dev/null
sleep 2
echo "  while SLOW-1 pending: SLOW-1=$(status_of SLOW-1), OTHER-2=$(status_of OTHER-2)"
echo "  SLOW-1 -> $(wait_decided SLOW-1), SLOW-2 -> $(wait_decided SLOW-2)"
docker compose exec -T api1 sh -c 'rm -f /data/raw/slow_batches.txt'

echo; echo "== 5. container crash mid-batch and survivor takeover =="
CDEV="${DEV}-CRASH"
# Ingest four batches first while both instances are up; the crash marker is
# only written afterwards so ingestion itself never crashes.
for n in 1 2 3 4; do csv "C$n" $([ "$n" = 4 ] && echo 504 || echo 500) 5 > "/tmp/c$n.csv"; done
C1ID=$(post CRASH-1 "$CDEV" /tmp/c1.csv | sed -n '1p' | python3 -c 'import sys,json;print(json.load(sys.stdin)["batch_id"])')
post CRASH-2 "$CDEV" /tmp/c2.csv >/dev/null
post CRASH-3 "$CDEV" /tmp/c3.csv >/dev/null
post CRASH-4 "$CDEV" /tmp/c4.csv >/dev/null

# Stop api1, then make a dedicated one-shot worker hard-exit while holding
# CRASH-1 (same effect as kill -9 of a container). api2 is the survivor.
docker compose stop api1
docker compose exec -T api2 sh -c "echo '$C1ID' > /data/raw/crash_batches.txt"
docker compose run --rm \
  -e INSTANCE_ID=crasher \
  api1 python -m scripts.worker_harness --instance-id crasher --crash "$C1ID" || true
echo "  row after crash (must stay PENDING, no median):"
curl -sS "$BASE/batches/CRASH-1" | j | grep -E 'status|median|attempts'
docker compose exec -T api2 sh -c 'rm -f /data/raw/crash_batches.txt'
docker compose start api1
for b in CRASH-1 CRASH-2 CRASH-3 CRASH-4; do echo "  $b -> $(wait_decided "$b")"; done
echo "  final decision + references for the post-baseline batch CRASH-4:"
curl -sS "$BASE/batches/CRASH-4" | j

echo; echo "== 6. data lake endpoint hides quarantined batches =="
curl -sS "$BASE/data/released?device_id=$DEV" | j | grep -E 'total_released|batch_no'

echo; echo "== 7. sealed snapshot: detail, freeze, idempotency, conflict =="
SDEV="${DEV}-SNAP"
for i in 1 2 3; do csv "N$i" 500 5 > "/tmp/n$i.csv"; post "SNAP-$i" "$SDEV" "/tmp/n$i.csv" >/dev/null; done
for i in 1 2 3; do echo "  SNAP-$i -> $(wait_decided "SNAP-$i")"; done
csv NQ 900 5 > /tmp/nq.csv; post SNAP-Q "$SDEV" /tmp/nq.csv >/dev/null
csv NP 502 5 > /tmp/np.csv; post SNAP-P "$SDEV" /tmp/np.csv >/dev/null
echo "  SNAP-Q -> $(wait_decided SNAP-Q), SNAP-P -> $(wait_decided SNAP-P)"

REQ="REQ-COMPOSE-$$"
seal() { curl -sS -o /tmp/seal_body.json -w '%{http_code}' -X POST "$BASE/snapshots" \
  -H 'Content-Type: application/json' \
  -d "{\"device_id\":\"$1\",\"start_accept_seq\":$2,\"end_accept_seq\":$3,\"request_no\":\"$4\"}"; }

first_code=$(seal "$SDEV" 1 4 "$REQ")
SNAP_NO=$(python3 -c 'import json;print(json.load(open("/tmp/seal_body.json"))["snapshot_no"])')
echo "  seal 1..4 -> HTTP $first_code snapshot=$SNAP_NO"
curl -sS "$BASE/snapshots/$SNAP_NO" | j | grep -E 'snapshot_no|released|quarantined|wafers|reference_batch_nos|status' | head -20
# Seed one more batch and seal it immediately.  It is normally still PENDING
# at seal time; the sealed response is captured and compared after the worker
# decides it, proving the by-number read is frozen either way.  (The pytest
# suite seals PENDING deterministically by pausing the worker.)
csv NPEND 504 5 > /tmp/npend.csv
post SNAP-PEND "$SDEV" /tmp/npend.csv >/dev/null
pend_code=$(seal "$SDEV" 5 5 "REQ-PEND-$$")
cp /tmp/seal_body.json /tmp/seal_pending.json
PEND_NO=$(python3 -c 'import json;print(json.load(open("/tmp/seal_body.json"))["snapshot_no"])')
PEND_STATUS=$(python3 -c 'import json;print(json.load(open("/tmp/seal_body.json"))["batches"][0]["status"])')
echo "  sealed seq5 while $PEND_STATUS -> HTTP $pend_code snapshot=$PEND_NO"
echo "  SNAP-PEND later -> $(wait_decided SNAP-PEND); re-read snapshot:"
curl -sS "$BASE/snapshots/$PEND_NO" -o /tmp/seal_reread.json
python3 - <<'PY'
import json
sealed = json.load(open("/tmp/seal_pending.json"))
again = json.load(open("/tmp/seal_reread.json"))
b = again["batches"][0]
print("   frozen status=%s wafers=%d totals=%s"
      % (b["status"], len(b["wafers"]), again["totals"]))
assert again == sealed, "snapshot changed after the batch was decided!"
print("   by-number read identical to the seal response")
PY
echo "  retry same request_no -> $(seal "$SDEV" 1 4 "$REQ")"
echo "  same request_no, different params -> $(seal "$SDEV" 1 3 "$REQ")"
echo "  range with unaccepted seq -> $(seal "$SDEV" 1 9 "REQ-GAP-$$")"
echo "  unknown snapshot -> $(curl -sS -o /dev/null -w '%{http_code}' "$BASE/snapshots/SNP-nope")"
echo "  other device still flows: $(post SNAP-OK "${DEV}-SNAP2" /tmp/n1.csv | tail -1) / $(wait_decided SNAP-OK)"

echo
echo "OpenAPI docs: $BASE/docs"

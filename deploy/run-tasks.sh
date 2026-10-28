#!/usr/bin/env bash
set -euo pipefail
: "${ECS_CLUSTER:?Set ECS_CLUSTER}"
: "${TASK_DEFINITION:?Set TASK_DEFINITION including revision}"
: "${NETWORK_CONFIG:?Set NETWORK_CONFIG to a JSON file path}"
TASK_COUNT="${TASK_COUNT:-5}"
if ! [[ "$TASK_COUNT" =~ ^[1-9][0-9]*$ ]]; then
  echo 'TASK_COUNT must be a positive integer' >&2
  exit 1
fi
# ECS accepts at most 10 tasks per RunTask call.
while (( TASK_COUNT > 0 )); do
  batch=$(( TASK_COUNT > 10 ? 10 : TASK_COUNT ))
  response_file="$(mktemp)"
  trap 'rm -f "$response_file"' EXIT
  aws ecs run-task --cluster "$ECS_CLUSTER" --task-definition "$TASK_DEFINITION" \
    --launch-type FARGATE --count "$batch" --network-configuration "file://$NETWORK_CONFIG" > "$response_file"
  python3 - "$response_file" <<'PY'
import json,sys
r=json.load(open(sys.argv[1]))
for task in r.get('tasks', []): print(task['taskArn'])
if r.get('failures'):
    print(json.dumps(r['failures']), file=sys.stderr)
    sys.exit(1)
PY
  rm -f "$response_file"
  trap - EXIT
  TASK_COUNT=$(( TASK_COUNT - batch ))
done

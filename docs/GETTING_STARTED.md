# Run the first alpha

Agent OS currently executes bounded Python and Node commands as Kubernetes Jobs. It is the first execution component for a broader agent environment: a coding agent, research harness, or workflow engine can submit work to it, but model calls, browser automation, durable workspaces, and checkpoint recovery are not integrated yet. Workload networking is denied by default, so the supplied examples work without provider credentials or internet access.

There are two ways to evaluate this alpha: inspect the exact Job specification locally, or execute it on a prepared Kubernetes cluster. Preview mode never executes submitted code.

## Inspect a Job without a cluster

Install Go 1.27.4, Python 3, and `curl`. From the repository root, create a tenant token and build:

```sh
export AGENTOS_LOCAL_TOKEN="$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')"
make build
./bin/agentos --preview --listen=127.0.0.1:8080 &
AGENTOS_PREVIEW_PID=$!
```

Keep the token private. After the server logs that it is listening, use the same terminal to inspect the templates and resolved Job:

```sh
curl --fail-with-body http://127.0.0.1:8080/healthz
curl --fail-with-body \
  -H "Authorization: Bearer ${AGENTOS_LOCAL_TOKEN}" \
  http://127.0.0.1:8080/v1/templates
curl --fail-with-body \
  -H "Authorization: Bearer ${AGENTOS_LOCAL_TOKEN}" \
  -H 'Content-Type: application/json' \
  --data-binary @examples/python-task.json \
  http://127.0.0.1:8080/v1/plan
```

The plan resolves an administrator-owned template into a Kubernetes Job with a sandbox RuntimeClass, resource bounds, a deadline, a non-root user, no service-account token, and bounded scratch volumes. A preview does not prove that a node implements gVisor or that a CNI enforces NetworkPolicy. Run creation and execution-log retrieval are unavailable in preview mode; listing stored records does not execute them. Stop the preview before using the same port for cluster access:

```sh
kill "$AGENTOS_PREVIEW_PID"
wait "$AGENTOS_PREVIEW_PID"
```

## Preview using the container image

After building with `make image`, the same API can run in a read-only container. With `AGENTOS_LOCAL_TOKEN` set in your shell:

```sh
docker run --detach --rm --name agentos-preview \
  --read-only --cap-drop=ALL --security-opt no-new-privileges \
  --tmpfs /var/lib/agentos:rw,noexec,nosuid,uid=10001,gid=10001,mode=0700 \
  --mount "type=bind,source=$(pwd)/config/example.json,target=/etc/agentos/config.json,readonly" \
  --env AGENTOS_LOCAL_TOKEN --publish 127.0.0.1:8080:8080 \
  agent-os:dev --preview --config=/etc/agentos/config.json \
  --db=/var/lib/agentos/state/runs.db --listen=:8080
```

Use the authenticated preview requests above from the same shell, then run `docker stop agentos-preview`. This preview has no Kubernetes connection; its temporary database disappears when the container is removed. The image runs as UID/GID 10001. The container preview and local preview are alternatives and cannot bind the same host port at once.

## Prepare a cluster for actual execution

Use a disposable development cluster first. You need:

- A Linux Kubernetes environment compatible with the client libraries in `go.mod`, and administrative access for the initial namespace, RBAC, and policy installation. No cluster compatibility matrix has been certified yet.
- The `runsc` gVisor handler installed and tested on the eligible worker nodes, exposed as a RuntimeClass named `gvisor`. The API checks the declared handler name and does not fall back to an ordinary runtime. It cannot verify the actual node binary or configuration. See the [gVisor Kubernetes installation guide](https://gvisor.dev/docs/user_guide/quick_start/kubernetes/).
- A CNI that enforces ingress and egress NetworkPolicy. Verify the intended isolation with actual traffic tests, including node services, metadata, DNS, IPv4/IPv6, and peer workloads. Other allow policies are additive. See the [Kubernetes NetworkPolicy documentation](https://kubernetes.io/docs/concepts/services-networking/network-policies/).
- A default storage class that supports a 1 GiB ReadWriteOnce claim and volume ownership through `fsGroup: 10001`, or a suitable pre-provisioned PV.
- `kubectl`, Docker or another OCI image builder, and `curl`. The examples below also use Python 3 locally.

If a correct `gvisor` RuntimeClass already exists, inspect and reuse it. `deploy/runtimeclass.example.yaml` is a separate example for an administrator who has installed `runsc`: it selects only nodes explicitly labeled `agentos.dev/gvisor-ready=true`. Creating the RuntimeClass or adding that label does not install or qualify a runtime. The example is excluded from `kubectl apply -k deploy`.

Before using untrusted tasks, verify the runtime and networking independently on your actual nodes. Image-pull access is handled by the node, separately from denied sandbox egress. Clusters with admission mutation, service-mesh sidecar injection, or custom scheduling behavior need qualification; this alpha rejects execution specifications whose protected fields have been changed.

## Install the API

Build the API image and make it available to the eligible API node:

```sh
make image
```

The manifests use `agent-os:dev` with `IfNotPresent`. Load this image into your local cluster using its documented image-loading mechanism. For a remote cluster, push a versioned image to your registry and edit `deploy/api.yaml` to use it; configure registry credentials as needed. This repository does not publish or deploy images automatically.

Create the namespaces, generate a fresh API token, and install the administrator configuration:

```sh
kubectl apply -f deploy/namespaces.yaml
export AGENTOS_LOCAL_TOKEN="$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')"
AGENTOS_SECRET_FILE="$(mktemp)"
chmod 600 "$AGENTOS_SECRET_FILE"
printf 'local-token=%s\n' "$AGENTOS_LOCAL_TOKEN" > "$AGENTOS_SECRET_FILE"
kubectl -n agentos-system create secret generic agentos-api-tokens \
  --from-env-file="$AGENTOS_SECRET_FILE" --dry-run=client -o yaml | kubectl apply -f -
rm -f "$AGENTOS_SECRET_FILE"
kubectl -n agentos-system create configmap agentos-config \
  --from-file=config.json=config/example.json --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -k deploy
```

Delete the temporary token file if an interrupted command leaves it behind. Do not commit generated Secrets or tokens. The configuration has one tenant, `local`, whose bearer token comes from `AGENTOS_LOCAL_TOKEN`. Add additional tenants through the administrator configuration and corresponding secret-backed environment variables, never through caller request fields.

The base API network policy permits DNS to conventional cluster DNS Pods and otherwise denies ingress and egress. **The API remains unready until you permit its Kubernetes API connection.** This destination is cluster-specific; a generic manifest cannot identify every control-plane endpoint or CNI service-translation path.

Inspect the service and backing endpoints:

```sh
kubectl -n default get service kubernetes -o wide
kubectl -n default get endpointslice -l kubernetes.io/service-name=kubernetes -o yaml
cp deploy/networking/kube-api-egress.example.yaml /tmp/agentos-kube-api-egress.yaml
```

Edit the copy to contain the actual service and endpoint CIDRs and API port, then apply it:

```sh
kubectl apply -f /tmp/agentos-kube-api-egress.yaml
kubectl -n agentos-system rollout status deployment/agentos-api --timeout=120s
```

In a separate terminal, start port-forwarding. This command does not need the API token:

```sh
kubectl -n agentos-system port-forward service/agentos-api 8080:8080
```

The example intentionally contains invalid placeholder CIDRs. Use `/128` for individual IPv6 addresses. Depending on the CNI, policy may match before or after service DNAT; the endpoint port may be `6443` rather than `443`. Some clusters require a CNI-specific policy for managed control-plane access. Node-local DNS also needs a different DNS rule. Use your CNI's documentation and verify reachability without broadening access to all destinations. `kubectl port-forward` typically uses the kubelet path rather than ordinary Pod ingress; verify this on your cluster. No Ingress, public load balancer, or TLS termination is supplied.

The readiness probe checks installation declarations, not proven isolation. A healthy response cannot establish that the CNI or runtime behaves correctly. The API has only namespace-scoped Job/Pod permissions plus named reads of the workload namespace and `gvisor` RuntimeClass. Workload Pods use a separate service account without API permissions or an automatically mounted token.

API concurrency limits do not guarantee cluster capacity. Keep template resources, admission limits, namespace quotas, and node capacity aligned when changing the examples. A Job rejected by quota or unable to start does not become a successful run.

## Execute a bounded task

Return to the installation terminal, where `AGENTOS_LOCAL_TOKEN` is already set. With port-forwarding running in the other terminal:

```sh
curl --fail-with-body http://127.0.0.1:8080/readyz
curl --fail-with-body \
  -H "Authorization: Bearer ${AGENTOS_LOCAL_TOKEN}" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: python-first-run' \
  --data-binary @examples/python-task.json \
  http://127.0.0.1:8080/v1/runs
```

Use the returned `id` to read status, logs, or cancel:

```sh
AGENTOS_RUN_ID=REPLACE_WITH_RETURNED_RUN_ID
curl --fail-with-body \
  -H "Authorization: Bearer ${AGENTOS_LOCAL_TOKEN}" \
  "http://127.0.0.1:8080/v1/runs/${AGENTOS_RUN_ID}"
curl --fail-with-body \
  -H "Authorization: Bearer ${AGENTOS_LOCAL_TOKEN}" \
  "http://127.0.0.1:8080/v1/runs/${AGENTOS_RUN_ID}/logs"
curl --fail-with-body -X POST \
  -H "Authorization: Bearer ${AGENTOS_LOCAL_TOKEN}" \
  "http://127.0.0.1:8080/v1/runs/${AGENTOS_RUN_ID}/cancel"
```

Logs become available after a Pod exists and are bounded. Terminal runs may already be complete when cancellation arrives. A cancellation request can remain `cancelling` while Kubernetes deletes the Job and Pod; API disappearance cannot prove physical termination on an unreachable worker. Cancellation cannot undo completed work. Do not force-delete Pods to demonstrate a stronger termination claim.

Repeat the same create request with the same idempotency key to retrieve the original run. Reusing a key with a different resolved request conflicts. An idempotency key prevents duplicate API admission; Kubernetes Jobs do not guarantee that arbitrary external effects occur exactly once. The alpha does not retry failed application work.

The caller selects an administrator-defined `template`, optional `command`, and optional `timeout_seconds`. Image, CPU, memory, scratch size, RuntimeClass, and the maximum deadline are template-controlled. Callers cannot submit arbitrary PodSpecs, volumes, environment credentials, or resource sizes. Templates pin workload images by digest. Commands run under the same constraints whether they represent a short task or one step in a larger agent workflow.

`timeout_seconds` is the activated Job's execution budget, enforced through Kubernetes `activeDeadlineSeconds`; it does not start at API admission. Scheduling and image-pull time after Job activation can consume this budget. Queued or not-yet-activated work has a separate fixed five-minute startup wait. Enforcement and physical termination are asynchronous, especially on unreachable nodes; neither bound promises an instantaneous stop.

`examples/node-task.json` exercises the Node template. `examples/timeout.json` sleeps past its two-second deadline; the Job should fail rather than run indefinitely. Treat these as cluster acceptance exercises to run, not evidence that this repository has already passed live cluster testing.

## State, logs, and restart limits

The API runs as one replica with an embedded bbolt database under `/var/lib/agentos/state/runs.db`. Keep the PVC and `Recreate` strategy. Multiple API replicas, a shared database opened by several processes, and horizontal control-plane scaling are unsupported. Accepted run metadata and state events survive an ordinary API restart when its database volume survives; this is not a persistent workload filesystem or a process checkpoint.

The controller reconciles recorded runs with their deterministic Job names after restart. A lost Job fails the run instead of silently re-executing it. Jobs are first created suspended, then activated through a persisted `activating` phase and a version-checked, noncreating operation. An initial create request that arrives late after cancellation can leave an inactive suspended Job; an operator may need to remove it. Workload scratch volumes disappear with their Pod. Logs are read from retained Kubernetes Pods and are not archived in bbolt. Retain finished Jobs until you have collected any required logs, then remove individual finished Jobs as an operator. The namespace Job-count quota also counts completed and suspended Jobs; manual housekeeping is necessary in this alpha.

For a database backup, stop admission, let active executions finish or cancel them, scale the API to zero, and take an offline database/PVC backup using your storage tooling. Keep workload Jobs and metadata consistent. Do not copy an open database as if it were an application-consistent backup. Restore procedures, disaster recovery, cross-cluster migration, and database schema upgrades have not been qualified for production.

Updating a ConfigMap or Secret does not reload the running process. Validate the new configuration, update it, and restart the single API deployment during a maintenance window. Treat changes to images or resource/deadline defaults as new template versions when clients depend on idempotent requests.

There is no enforced model-spend budget, outbound model/tool gateway, warm pool, checkpoint integration, durable workspace, or measured density/token-cost improvement yet. See [ROADMAP.md](ROADMAP.md) for the next capability gates and [SECURITY.md](../SECURITY.md) for the current trust boundary.

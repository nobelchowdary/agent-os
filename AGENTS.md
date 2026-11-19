# Engineering guidance

This repository is an experimental Kubernetes sandbox environment for agent work. Read README.md and docs/decisions/0001-initial-execution-slice.md before extending runtime behavior.

- Keep caller input separate from administrator-owned environment policy. Never accept arbitrary PodSpecs, host mounts, device access or workload credentials through the run API.
- Require the selected sandbox runtime. Never introduce a fallback to ordinary containers or execute submitted agent code on the API host.
- Preserve tenant scoping, durable admission, transactional quotas, idempotent submission and terminal-state immutability.
- Treat Kubernetes creation, activation and cancellation as operations with ambiguous failure outcomes. Add a behavioral test when changing these contracts.
- A runtime name, manifest, fake client test or successful build is not evidence of real node isolation or performance. State which checks actually ran.
- Keep secrets, databases, kubeconfigs and generated binaries out of Git. Do not print API credentials in logs or test output.
- Run `make check` for implementation changes. Verify packaging when it changes. Keep production and performance claims tied to measured evidence.
- Use the roadmap to connect new features to workloads and acceptance criteria. Prefer existing sandbox/lifecycle components to a new scheduler or hypervisor.

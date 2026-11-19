# Contributing

Agent OS is an early execution-runtime project. Start with [README.md](README.md), [GETTING_STARTED.md](docs/GETTING_STARTED.md), and the current [roadmap](docs/ROADMAP.md). The product direction supports several kinds of agents; the first implementation handles bounded Kubernetes Jobs.

Use Go 1.27.4 and run:

```sh
make check
```

For packaging changes, also build the image and render the manifests where the tools are available:

```sh
make image
make manifests
```

Describe the behavior changed, its reason, how it was verified, and the environment used. Keep local credentials, kubeconfigs, generated Secrets, databases, logs, and customer data out of contributions. Report security issues through the process in [SECURITY.md](SECURITY.md).

Protect the execution contract: tenant identity comes from authentication; callers cannot supply raw Kubernetes specifications; administrator templates control images and resources; sandbox requirements fail closed; records survive API restart; retries and cancellation never imply an exactly-once external-effects guarantee. Changes to these boundaries need focused tests and an explicit explanation of the new behavior.

Unit tests and Kubernetes fake-client tests are useful evidence for request handling and object construction. Label them accurately. A claim about actual isolation, scheduling, cleanup, startup latency, density, or cost also needs a reproducible live environment and measured results. Use [EVALUATION_PLAN.md](docs/EVALUATION_PLAN.md) to structure such work.

Discuss major dependencies, new sandbox backends, or API contract changes before investing in a large patch. Prefer a small, reviewable capability tied to a workload over a speculative subsystem. New runtime integrations must document host prerequisites and failure behavior.

By contributing, you confirm that you have the right to submit the change and license it under the repository's [Apache-2.0 license](LICENSE). Preserve third-party attribution and explain any new dependency license requirements. No contribution agreement or response-time commitment has been established yet.

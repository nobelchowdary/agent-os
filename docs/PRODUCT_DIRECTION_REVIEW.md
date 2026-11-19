# Product direction review: efficient execution and execution control

Date: November 18, 2026. Status: exploration proposal; no product pivot approved.

Updated November 19 with the [alpha capability map](ALPHA_CAPABILITY_MAP.md) and [EKS evaluation brief](EKS_EVALUATION_BRIEF.md). The founder can set up EKS. The next engineering step is qualification and comparison with upstream implementations, while both product directions remain under investigation.

The follow-up research correctly identifies OpenShell's CNCF contribution, but overstates several remaining gaps. Agent Substrate has a 1.0 release and existing non-GKE checkpoint/restore code; OpenShell has suspend/resume APIs with driver limitations; AWS documents a per-user session pattern for cross-conversation limits. The capability map contains sources and the distinction between implemented mechanisms, supported deployments and results we have actually measured.

This review compares the founder-supplied research with current primary documentation and the working alpha. The founder can reach Kubernetes platform teams and wants to explore both a sandbox-centered product and a broader execution-control product before choosing. The implementation and accepted runtime decision remain unchanged.

## 1. Recommendation

Explore two concrete customer problems with the same platform-team audience. Retain the sandbox and hardware-efficiency ambition while testing whether enforceable task controls are the more valuable product. Do not expand into enterprise-wide agent inventory, every SaaS integration, or universal recovery before there is customer evidence.

The original plan already included policy, budgets, mediated credentials and recovery. The new research sharpens prioritization; it does not establish that the project must abandon its existing execution foundation.

Working positioning to test: **run agent tasks in the customer's Kubernetes environment with measurable execution economics and explicit controls over permissions, resource use and recovery.** Every claimed advantage remains a hypothesis.

This is a candidate problem statement, not differentiated public positioning. Existing projects describe overlapping capabilities. Publish a narrower claim only after demonstrating a customer-relevant advantage against them.

## 2. Corrections that affect the decision

| Topic | What current primary documentation supports | Product consequence |
| --- | --- | --- |
| OpenShell positioning | Beyond the report's developer-machine characterization, OpenShell documents Kubernetes operation, trusted supervisors, mediated networking and credential handling across supported runtimes | Treat it as a direct alternative and potential foundation; assess integration before recreating its controls. [Architecture](https://docs.nvidia.com/openshell/latest/about/architecture) |
| Third-party inventory | Supported external platforms can be synchronized into Agent 365; synchronization is currently manual, coverage varies, and connectors are primarily read-only | The report's automatic-inventory qualifier matters, but it should not imply external visibility is absent. Separate inventory from inline enforcement. [Connected platforms](https://learn.microsoft.com/en-us/microsoft-agent-365/admin/connected-platforms) |
| Vendor-neutral policy | AWS documents AgentCore Gateway policy outside agent code, and Identity supports self-hosted/hybrid workload identities; OpenShell also documents controls across runtimes | A framework-independent gateway alone is insufficient differentiation. [Policy](https://aws.amazon.com/about-aws/whats-new/2026/03/policy-amazon-bedrock-agentcore-generally-available/), [identity](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/understanding-agent-identities.html) |
| Enforced budgets | Beyond dashboards, AgentCore documents session-scoped temporal policy controls and their limitations | Investigate a specific aggregate root-task budget spanning sessions/runtimes; do not claim all enforced limits are absent. [Temporal policy](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/policy-temporal.html) |
| Tool gateway coverage | Docker documents gateway policy and the need to close direct connection bypasses | The product must prove mediation coverage and explicitly identify ungoverned paths. [MCP policy](https://docs.docker.com/ai/governance/organization/policies/mcp/) |

These are documented product capabilities, not independent validation of their security, performance, or customer outcomes. They invalidate an assumption of an empty category; they do not settle whether customers are well served.

The pasted report's pricing, revenue, financing, acquisition, survey and regulatory figures lack complete primary-source citations in the supplied text. This review does not adopt those numbers or use them for valuation. Matching list prices alone would not establish commoditization, equal service quality, or an inability to charge for a specialized workload.

## 3. Two options to investigate

| | A: efficient agent execution | B: task controls across runtimes |
| --- | --- | --- |
| First buyer | Kubernetes platform engineering | Platform engineering with security as reviewer; test whether another budget owner is required |
| Customer problem | Idle capacity, slow environments, fragile recovery or high operating effort | Inconsistent permissions, credential exposure, fragmented task budgets or slow revocation across execution paths |
| Proposed product | A supported, benchmarked execution environment using existing isolation/lifecycle components | An independently enforced task identity, policy, budget and action record across two supported runtime integrations |
| Evidence required | Better total cost or useful throughput under matched isolation, quality, latency and failure requirements | Demonstrated coverage, revocation of future actions, concurrent accounting correctness and acceptable overhead; customers accept required routing changes |
| Main risk | Existing substrates already perform adequately or savings disappear after operations | Broad integration surface, bypass paths, overlapping competitors and a different enterprise purchase process |

The options can share a foundation without being sold as one giant platform. Neither option should be expanded until a customer can explain why the result is worth deploying and funding.

## 4. Reuse the current alpha

Retain administrator-owned templates, run identity, tenant isolation, admission transactions, idempotency, bounded execution and the stage/activate/cancel tests. They provide a reference integration and regression harness.

For option A, qualify EKS and one real workload, then compare the existing Job backend with upstream Agent Sandbox including warm pools. Evaluate Agent Substrate (Ate) separately for its existing stateful checkpoint/restore behavior, and OpenShell for policy/credential integration. A warm fresh Pod and a restored agent's memory are different outcomes. Reuse a substrate if it meets the contract; no custom scheduler or OS distribution is justified yet.

For option B, prototype a narrow external broker before building a management UI. First support one model-provider adapter, one controlled write-tool adapter, and two execution integrations selected from actual partner deployments. The same controller over two language examples is not proof of cross-runtime support.

First assess OpenShell's released extension points and APF's explicitly marked development capabilities. Make OpenShell on Kubernetes one proposed integration/baseline; build a custom broker only for a demonstrated unmet contract. Do not assume absent native approvals, immediate revocation or accounting from an index search alone.

The broker would validate server-issued task identity, delegated permissions, root-task budget reservations and operation identity before forwarding a request. Keep reusable upstream credentials out of agent workloads. Include parent/child work in a shared allowance and deny new billable operations if authorization/accounting is unavailable. Use existing policy/identity components when suitable.

This is a future experiment. The current API has shared tenant bearer tokens, not this workload identity or broker. No implementation change is implied by this review.

## 5. Claims to make precise

- **Enforcement coverage:** distinguish actions passing through a verified broker from observed-only or uncontrolled actions. Require network/credential constraints that close alternative paths; prompts and SDK cooperation alone do not establish containment.
- **Revocation:** measure when a committed revoke starts denying new privileged requests. Report any propagation window and separate already submitted/in-flight actions. Stopping a process is not undoing an external change.
- **Recovery:** preserve workspace/checkpoint state at supported safe points. External writes require staged approval, upstream idempotency or connector-specific compensation. Deleted secrets, disclosed data, sent messages and irreversible transactions do not have universal undo.
- **Budgets:** reserve a defensible upper bound for supported requests, include concurrent children, reconcile usage, and disclose provider-invoice uncertainty. Adapters without a bound cannot promise a strict dollar cap.
- **Token efficiency:** distinguish fewer duplicated calls, less retry waste, tool-discovery/context improvements and changes to the model/harness. The runtime alone cannot guarantee better reasoning or fewer required tokens. Measure output quality and total task cost alongside tokens.

An action journal records attempted, committed, failed and uncertain outcomes; it does not make an ambiguous external request exactly once. A runtime memory snapshot does not roll back another system.

## 6. One comparison protocol

Choose a real coding task with idle gaps and a research/tool task with a controlled write step. Run the same inputs and output checks on the customer's current deployment and an optimized existing alternative. Keep model, harness, context strategy, isolation requirements and enabled controls matched for each comparison.

For option A, measure useful completed tasks per node, CPU/RAM consumption, idle capacity, readiness latency, state size, failed/lost work and operational effort. Include the control plane, warm capacity, storage and networking in cost. Report cold and warm distributions separately.

For option B, measure policy-decision overhead, allowed-task completion/false blocking, revocation propagation, bypass attempts, credential exposure checks, accounting under concurrent requests, and ambiguous-outcome handling. Use the same task on two actual runtime integrations. A finite test suite supports only its tested cases.

Use failure injection for broker/database loss, controller restart, worker loss, delayed acknowledgments, expired credentials and unsupported protocols. A security demonstration that blocks all legitimate work is not a successful product result.

Use a deterministic mock model for infrastructure measurements and separate repeated real-model trials for task completion, quality and token consumption. Do not attribute a cheaper model, shorter answer or omitted work to runtime optimization.

Pre-agree a meaningful benefit with each partner rather than choosing a marketing threshold after seeing results. The earlier evaluation plan's numerical gates remain proposed engineering targets, not achieved advantages.

## 7. Customer evidence and selection gate

Run approximately 10–15 qualified interviews, seek at least five independent descriptions of the same concrete problem, and obtain two or three partners willing to provide sanitized traces and install an experiment. These are proposed discovery gates, not market statistics.

Choose A if customers repeatedly identify execution economics/reliability as a funded problem and matched tests show a material advantage. Choose B if they require consistent controls across actual runtime deployments, can grant the necessary enforcement access, and will fund that outcome even after comparison with OpenShell and existing gateways.

Consider a combined product only when the same buyer needs both and can operate it without disproportionate integration work. If neither experiment produces a meaningful advantage or willingness to pay, narrow or change the problem before adding features.

## 8. Research requested from the founder

For each reachable platform team, collect the following in sanitized form:

1. **A real workload and problem:** what agent/harness does it run, what was the last expensive, failed or unsafe task, and what did the failure cost in engineering time or spend?
2. **Deployment and traces:** cloud/Kubernetes distribution, sandbox technology, concurrency, CPU/RAM/idle time, environment startup, state size, task completion rate, provider usage and current costs. Estimates must be labeled.
3. **Existing alternatives:** what have they tried, why did OpenShell/Ate/Agent Sandbox/a managed service not satisfy them, and which limitation can be reproduced?
4. **Enforcement access:** can they route model/tool calls through a broker, withhold direct provider credentials, restrict outbound paths, and tolerate policy failures blocking new operations? Which protocols cannot be mediated?
5. **Commercial commitment:** who owns the budget, what acceptance criteria would justify a paid pilot, what installation/support burden is acceptable, and what would turn the pilot into a recurring purchase?
6. **Recent upstream evaluation:** have they evaluated OpenShell or Agent Substrate since KubeCon, using which release/configuration, and what concrete requirement still failed? Reproduce that limitation before calling it a product opportunity.

Useful competitor research should supply the exact primary URL, publication/version date, a reproducible supported workflow, the stated limitation and whether the claim is independently tested. Avoid another broad market-size summary without deployment evidence.

Suggested research prompt: "Compare the latest documented OpenShell, Agent Sandbox, Ate and the customer's current stack for this specific workload. Identify support and limitations for aggregate parent/child task budgets, credential mediation, revocation, durable workspace recovery and network bypass prevention. Use official documentation and source code, identify what is implemented versus planned, and propose a reproducible test for every alleged gap. Do not infer absence from an omitted marketing page or estimate our valuation from funding rounds."

## 9. Acquisition expectations

A multimillion-dollar sale is possible; no benchmark alone establishes an acquisition floor or probability. Benchmarks support technical differentiation. Customer deployments and recurring use support usefulness. Paid contracts and renewals support willingness to pay. A prospective acquirer still needs a reason to buy rather than build, license or partner.

Fundraising, annual recurring revenue and an acquisition purchase price are different outcomes. Comparable financing announcements or large-company deals do not price this project. Build repeatable customer value, maintainable technology, clear contribution ownership and a specific integration advantage; preserve the ability to run an independent business.

The next decision is which customer problem to validate first, not what sale price to promise.

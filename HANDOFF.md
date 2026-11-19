# Handoff: Flexera Spot (Ocean) for AKS - Learning Track

Give this file to any AI assistant and ask it to read this file first, then the docs it points to.

---

## Who I am and what I want

- I'm Abhi. My office is adopting **Flexera Spot (Ocean)** on **AKS**, most likely to **reduce cost** by running workloads on Azure Spot VMs.
- I didn't know the topic, so I'm learning it **hands-on** while working, so I can follow and contribute to the office adoption.
- Style I like: step-by-step docs with **"What to check"**, **"Concept"** boxes, self-check questions, and a `doubts/` folder per phase for my questions and the answers. Same format as my other project, `kafka-gitops-platform/docs/`.

## Rules for the assistant

1. **One phase at a time.** Help me finish the current phase before moving on. Don't dump all phases at once.
2. When I hit an error or a question, answer it and **save the Q&A** in `docs/Phase-N/doubts/<n>.<topic>.md`.
3. If a doc command turns out to be wrong (the playground blocks it, the Flexera console differs, flags changed), **fix the phase doc itself** and mention the fix to me.
4. Keep explanations simple and practical. Link things back to what I'd see or do at the office.
5. Don't create extra docs I didn't ask for.

---

## Environment decision (already agreed)

| Phases | Where | Why |
|---|---|---|
| 0-3 | **KodeKloud Azure playground** (real AKS, free, time-limited, restricted permissions) | Native AKS Spot basics |
| 4 | **Personal Azure subscription** (Pay-As-You-Go) plus a Flexera Spot account | Ocean needs an Entra ID app registration and a custom role, which the playground likely blocks |
| 5 | Reading / any cluster | Classifying workloads |

Personal subscription cost guards: budget alert of about 10 USD, AKS Free tier, max 2 Ocean nodes, and teardown the same day (all listed in Phase 4).

---

## Status (as of 19 Nov 2026)

All docs are **written**. **None have been executed yet.** The commands are untested, so expect small fixes.

| Phase | File | Status |
|---|---|---|
| Plan + Q&A | `docs/0.plan-and-qa.md` | Done (read it for context) |
| 0 - Concepts | `docs/Phase-0/0.concepts.md` | **NEXT: start here** |
| 1 - AKS + Spot node pool | `docs/Phase-1/1.aks-spot-nodepool.md` | Not started |
| 2 - Workloads on Spot | `docs/Phase-2/2.workloads-on-spot.md` | Not started |
| 3 - Simulate eviction | `docs/Phase-3/3.simulate-eviction.md` | Not started |
| 4 - Real Ocean onboarding | `docs/Phase-4/4.ocean-onboarding.md` | Not started |
| 5 - What not to put on Spot | `docs/Phase-5/5.what-not-to-put-on-spot.md` | Not started |
| Office questions | `docs/office-questions.md` | Fill in as I learn at work |

**Update this table** as phases finish.

---

## Key points already established (don't re-explain from zero)

- Flexera Spot = the former **Spot by NetApp** (`spotinst` in names). Its AKS product is **Ocean**: a controller in the cluster plus a SaaS that calls the Azure API to create and remove Spot or on-demand VMs, pick VM sizes, predict evictions and fall back to on-demand.
- Ocean **replaces** manual Spot node pools and the AKS cluster autoscaler, which must be disabled. It does **not** replace workload resilience: replicas, PDBs, graceful shutdown, tolerations and affinity are still our job. That's why Phases 1-3 come before Ocean.
- AKS Spot nodes get the taint `kubernetes.azure.com/scalesetpriority=spot:NoSchedule` and the matching label. The system pool can't be Spot.
- **B-series VMs aren't supported for Spot**, so the Spot pool uses `Standard_D2s_v3` (with alternatives in Phase 1).
- **Preferred** Spot affinity can leave pods on on-demand nodes: the autoscaler never scales Spot up, and pods don't move back. Ocean solves this.
- A PDB protects against voluntary drains, **not** against Azure's 30-second eviction.
- Kafka, databases and singletons stay **off** Spot (zone-locked disks, ISR, quorum). This links to my Kafka project's zone notes.

## Things still to verify (unconfirmed in the docs)

- Whether the KodeKloud Azure playground allows Spot quota, the allowed VM sizes, and access to the `MC_*` resource group (needed for `az vmss simulate-eviction`). Phase 3 has a `kubectl drain` fallback.
- The current Flexera Spot console flow, Helm chart name (`ocean-kubernetes-controller` vs the older `spotinst-kubernetes-cluster-controller`), and whether a free trial exists.
- Whether `spotinst.io/node-lifecycle: od` and `spotinst.io/restrict-scale-down` apply to **AKS** in the current Ocean version.
- Whether Ocean-launched AKS Spot nodes carry the same `scalesetpriority` taint and label.

---

## Prompt to paste into the office AI

> Read `flexera-spot-aks/HANDOFF.md` and then `flexera-spot-aks/docs/0.plan-and-qa.md`. I'm continuing a hands-on learning track on Flexera Spot Ocean for AKS. Follow the rules in HANDOFF.md: one phase at a time, save my doubts in `doubts/`, and fix the docs if a command is wrong. I'm starting **Phase 0** now; quiz me with its self-check questions when I'm done reading.

(Change "Phase 0" to whichever phase you're on.)

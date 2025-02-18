---
title: Security architecture
headerTitle: Security architecture
linkTitle: Security architecture
description: Security architecture of YugabyteDB Managed.
image: /images/section_icons/index/secure.png
headcontent: Review the YugabyteDB Managed security architecture and shared responsibility model.
section: YUGABYTEDB MANAGED
menu:
  preview:
    identifier: cloud-security
weight: 800
---

YugabyteDB Managed is a fully managed YugabyteDB-as-a-Service that allows you to run YugabyteDB clusters on public cloud providers such as Google Cloud Platform (GCP) and Amazon Web Services (AWS).

YugabyteDB Managed uses a shared responsibility model, where security and compliance is a shared responsibility between public cloud providers, Yugabyte, and YugabyteDB Managed customers.

The YugabyteDB Managed architecture is secure by default, and uses the following features to protect clusters and communication between clients and databases:

- encryption in transit
- encryption at rest
- limited network exposure
- authentication
- role-based access control for authorization

For information on how to configure the security features of clusters in YugabyteDB Managed, refer to [Secure clusters in YugabyteDB Managed](../cloud-secure-clusters/).

<div class="row">
  <div class="col-12 col-md-6 col-lg-12 col-xl-6">
    <a class="section-link icon-offset" href="cloud-security-features/">
      <div class="head">
        <img class="icon" src="/images/section_icons/secure/checklist.png" aria-hidden="true" />
        <div class="title">Security architecture</div>
      </div>
      <div class="body">
        Learn about the YugabyteDB Managed security architecture.
      </div>
    </a>
  </div>

  <div class="col-12 col-md-6 col-lg-12 col-xl-6">
    <a class="section-link icon-offset" href="shared-responsibility/">
      <div class="head">
        <img class="icon" src="/images/section_icons/secure/grant-permissions.png" aria-hidden="true" />
        <div class="title">Shared responsibility model</div>
      </div>
      <div class="body">
        The YugabyteDB Managed shared responsibility model for security.
      </div>
    </a>
  </div>

</div>

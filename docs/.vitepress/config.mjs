export default {
  title: 'The Book of Astra',
  description: 'Canonical knowledge base for Astra architecture, operations, migration, and internals.',
  lastUpdated: true,
  cleanUrls: true,
  themeConfig: {
    nav: [
      { text: 'Quickstart', link: '/quickstart' },
      { text: 'Guides', link: '/guides/migration-omni' },
      { text: 'Operations', link: '/operations/deploy-production' },
      { text: 'Reference', link: '/reference/cli/astrad' },
      { text: 'Internals', link: '/internals/architecture-overview' },
      { text: 'Contribute', link: '/contribute/contributing' }
    ],
    sidebar: [
      {
        text: 'Start Here',
        items: [
          { text: 'Overview', link: '/' },
          { text: 'Quickstart', link: '/quickstart' }
        ]
      },
      {
        text: 'Concepts',
        items: [
          { text: 'Control-Plane Compatibility', link: '/concepts/control-plane-compatibility' },
          { text: 'Consistency Model', link: '/concepts/consistency-model' },
          { text: 'Tenant Virtualization', link: '/concepts/tenant-virtualization' }
        ]
      },
      {
        text: 'Guides',
        items: [
          { text: 'Migrate Omni', link: '/guides/migration-omni' },
          { text: 'Migrate K3s', link: '/guides/migration-k3s' },
          { text: 'Deploy Astra + K3s', link: '/guides/deploy-k3s-with-astra' },
          { text: 'Migrate Generic etcd', link: '/guides/migration-etcd-generic' },
          { text: 'Disaster Recovery', link: '/guides/disaster-recovery' },
          { text: 'Performance Tuning', link: '/guides/performance-tuning' }
        ]
      },
      {
        text: 'Operations',
        items: [
          { text: 'Deploy Local Compose', link: '/operations/deploy-local-compose' },
          { text: 'Deploy Production', link: '/operations/deploy-production' },
          { text: 'Profiles and Auto-Governor', link: '/operations/profiles-and-auto-governor' },
          { text: 'Auth and Tenanting', link: '/operations/auth-and-tenanting' },
          { text: 'Backup and Restore', link: '/operations/backup-and-restore' },
          { text: 'Monitoring', link: '/operations/monitoring-prometheus-grafana' },
          { text: 'Troubleshooting', link: '/operations/troubleshooting' },
          { text: 'FAQ', link: '/operations/faq' }
        ]
      },
      {
        text: 'Reference',
        items: [
          { text: 'astrad CLI', link: '/reference/cli/astrad' },
          { text: 'astra-forge CLI', link: '/reference/cli/astra-forge' },
          { text: 'astractl CLI', link: '/reference/cli/astractl' },
          { text: 'etcd KV/Watch/Lease API', link: '/reference/api/etcd-kv-watch-lease' },
          { text: 'Astra Admin API', link: '/reference/api/astra-admin-bulkload' },
          { text: 'Internal Raft API', link: '/reference/api/internal-raft' },
          { text: 'Environment Variables', link: '/reference/config/env-vars' },
          { text: 'Docker Compose Reference', link: '/reference/config/docker-compose-reference' },
          { text: 'Metric Catalog', link: '/reference/metrics/metric-catalog' },
          { text: 'SLI/SLO Reference', link: '/reference/metrics/slo-sli-reference' }
        ]
      },
      {
        text: 'Internals',
        items: [
          { text: 'Architecture Overview', link: '/internals/architecture-overview' },
          { text: 'Write Path', link: '/internals/write-path' },
          { text: 'Read Path', link: '/internals/read-path' },
          { text: 'Watch Ring', link: '/internals/watch-ring' },
          { text: 'Raft Timeline', link: '/internals/raft-timeline' },
          { text: 'Tiering and S3', link: '/internals/tiering-and-s3' },
          { text: 'Memory and Backpressure', link: '/internals/memory-and-backpressure' },
          { text: 'QoS and Lanes', link: '/internals/qos-and-lanes' },
          { text: 'Validation Harness', link: '/internals/testing-and-validation-harness' }
        ]
      },
      {
        text: 'Contribute',
        items: [
          { text: 'Contributing', link: '/contribute/contributing' },
          { text: 'Docs Style Guide', link: '/contribute/docs-style-guide' },
          { text: 'Docs Sources of Truth', link: '/contribute/docs-source-of-truth' },
          { text: 'ADR Index', link: '/contribute/adr-index' }
        ]
      }
    ]
  }
}

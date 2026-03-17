# CryptoScope Source Registry

All data sources are defined as YAML files in `config/sources/`. Each file covers a category:

| File | Category | Count |
|------|----------|-------|
| `exchanges.yaml` | CEX & DEX market data | 40+ |
| `onchain.yaml` | On-chain analytics & block explorers | 40+ |
| `defi_protocols.yaml` | Individual protocol feeds | 100+ |
| `academic.yaml` | Academic papers & research labs | 30+ |
| `github_repos.yaml` | GitHub repos to monitor | 200+ |
| `research_firms.yaml` | Industry research & VC blogs | 25+ |
| `news_media.yaml` | News outlets (EN + ZH) | 25+ |
| `newsletters.yaml` | Substacks & individual blogs | 15+ |
| `governance.yaml` | Snapshot spaces & Discourse forums | 25+ |
| `regulatory.yaml` | SEC, CFTC, BIS, IMF feeds | 10+ |
| `twitter_lists.yaml` | Curated X/Twitter lists | 200+ accounts |
| `telegram_channels.yaml` | Public Telegram channels | 10+ |
| `podcasts.yaml` | Podcast RSS feeds | 10+ |

## Adding a new source

```bash
python scripts/add_source.py
```

Or manually add to the appropriate YAML file following the format:

```yaml
- id: unique_slug
  name: Display Name
  type: rss|api|scrape|db
  url: https://...
  collector: news_feed|chain_data|github_tracker|...
  frequency: 15m|1h|6h|24h
  category: news|research|academic|...
  priority: high|medium|low
  enabled: true
```

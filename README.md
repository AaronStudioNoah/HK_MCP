# HK CRM Full Data Dump

Full JSON dump of Noah HK CRM MCP server (`crm-customers-hk`, server v2.14.7), region=HK.

## Layout

```
data/
  customers/
    list/page_0001.json ... page_0175.json    # 87,437 customers, all list fields, 500/page
    list/_index.json                          # consolidated group_no -> record (gitignored, regenerate)
    details/<group_no>/
      kyc.json                                # hk_get_customer_kyc
      domestic_holdings.json                  # hk_get_domestic_holdings
      hk_holdings.json                        # hk_get_hk_holdings (HK assets + US insurance + HK trust + FCN)
      sg_holdings.json                        # hk_get_sg_holdings (SG assets + identity + SG trust)
      service_records.json                    # hk_get_service_records
      live_information.json                   # hk_get_live_information
  stats/
    rm_stats_total.json                       # default region=HK
    rm_stats_hk_opened.json                   # hongkongServiceFlag=是
    rm_stats_hk_not_opened.json               # hongkongServiceFlag=否
    rm_stats_self_developed.json              # selfDevelopedFlag=是
    rm_stats_referral.json                    # selfDevelopedFlag=否
    rm_stats_by_level_<level>.json            # 客户等级切片
    rm_stats_by_star_<0..10>.json             # 星级切片
```

## Field semantics

All list records include the original CRM camelCase fields. Field meaning is documented in the source MCP tool description (`fieldDescriptions` key in any list page response).

Notes carried over from upstream:
- Customer name and phone numbers are stripped by the upstream server.
- AUM / 创收 / 入金 / 估值 are in original currency per field description; do not auto-convert.

## Reproduce

```bash
pip install httpx
python3 scripts/download_all.py --stage all --concurrency 48
```

Resume-safe: existing non-empty output files are skipped. To force re-download of a customer, delete its directory under `data/customers/details/`.

## Scope

- 87,437 HK customers (full universe at dump time)
- Stats: 30+ aggregation slices
- Meetings: empty (current account has 0 meetings)
- CIO reports: excluded by request

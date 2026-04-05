# runtoolsio-sns

SNS notification plugin for runtools. Publishes AWS SNS notifications on job lifecycle events.

## Configuration

Add to environment config via `taro env edit`:

```toml
[[plugins.sns.rules]]
term_status = "failed"
topic_arn = "arn:aws:sns:eu-west-1:123:alerts"
format = "slack"

[[plugins.sns.rules]]
stage = ["running", "ended"]
topic_arn = "arn:aws:sns:eu-west-1:123:lifecycle"
```

### Rule fields

- **`stage`** — lifecycle stage(s) to match: `created`, `running`, `ended` (default: `ended`)
- **`outcome`** — match by outcome group: `success`, `fault`, `aborted`, `rejected`, `ignored`
- **`term_status`** — match by specific status: `completed`, `failed`, `stopped`, `timeout`, etc.
- **`topic_arn`** — SNS topic ARN (required)
- **`format`** — message format: `json` (default) or `slack`

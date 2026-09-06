# Redis transport plan

## Purpose

Redis is the optional transport between the local research client and the Big QMT bridge. The data-lake remains read-only; no account, order, or trade command is enabled by this plan.

## Local topology

Use the existing WSL Ubuntu Redis service, kept alive by `D:\QMT\local-data-lake\scripts\start-wsl-redis.ps1`. Keep Redis bound to localhost and use the configured port (currently the backup helper expects `6380`). Do not expose the service to a LAN or the Internet.

```
local research scripts -> Redis on WSL localhost -> QMT bridge -> QMT terminal
```

## Private configuration

The QMT-side private configuration must remain outside this repository, for example `D:\Programs\iQuant\python\bigqmt_signal_trader_local_config.py`:

```python
BIGQMT_REDIS_CONFIG = {
    "host": "127.0.0.1",
    "port": 6380,
    "db": 5,
    "password": "set-locally-only",
    "rpc_allow_order_methods": False,
}
```

Keep the password in that private file or in an OS-managed secret store. Never put it in Git, environment dumps, logs, issue text, or the scheduled sync output.

## Verification

Before enabling a client workflow, confirm only the read path:

```powershell
python D:\QMT\vendor\xtquant_big_convert-main\qmt-trader\scripts\qmt.py ping
```

Only an explicit later decision may set `rpc_allow_order_methods` to `True`.

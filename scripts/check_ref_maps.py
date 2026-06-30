"""Validate get_ref_maps() resolves option_by_ref against the local master cache."""
import sys
sys.path.insert(0, ".")
from app.core.env_loader import load_project_env
load_project_env(".")

from app.instruments.manager import InstrumentManager
import pandas as pd

# Inject the local master CSV directly as the fetcher to bypass live auth.
def _csv_fetcher() -> pd.DataFrame:
    return pd.read_csv(r"c:\trading_code\Nubra_live\instrument_master_cache.csv", low_memory=False)

mgr = InstrumentManager(
    env_name="PROD",
    use_env_creds=False,
    local_cache_csv=None,
    strike_radius=15,
    instrument_fetcher=_csv_fetcher,
)

print("strike_scale =", mgr._strike_scale)
print("strike_step  =", mgr._strike_step)
for px in (24500.0, 23900.0, 23850.0):
    st = mgr.reference_map_status(px)
    print(f"\nnifty_price={px}")
    for k, v in st.items():
        print(f"  {k} = {v}")

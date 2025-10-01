import pandas as pd
from rvol_vslice import add_session_and_index
from rvol_vslice import compute_rvol  # <- not compute_rvol_simple


def test_bar_idx_resets_each_day():
    df = pd.DataFrame({
        "timestamp": pd.to_datetime([
            "2025-09-01 09:30","2025-09-01 09:31",
            "2025-09-02 09:30","2025-09-02 09:31"
        ]),
        "open":[0,0,0,0],"high":[0,0,0,0],"low":[0,0,0,0],"close":[0,0,0,0],
        "volume":[1,2,3,4]
    })
    out = add_session_and_index(df)
    assert out["bar_idx"].tolist() == [0,1,0,1]

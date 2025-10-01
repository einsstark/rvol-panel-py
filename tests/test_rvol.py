import pandas as pd
from rvol_vslice import add_session_and_index
from rvol_vslice import compute_rvol  # <- not compute_rvol_simple

def test_bar_rvol_equals_one_when_volume_equals_baseline():
    # Two identical sessions -> baseline equals today's bar -> barRV == 1
    df = pd.DataFrame({
        "timestamp": pd.to_datetime([
            "2025-09-01 09:30","2025-09-01 09:31",
            "2025-09-02 09:30","2025-09-02 09:31",
        ]),
        "open":[0,0,0,0],"high":[0,0,0,0],"low":[0,0,0,0],"close":[0,0,0,0],
        "volume":[100,200,100,200]
    })
    df = add_session_and_index(df)
    out = compute_rvol(df, days=1)
    got = out[out["session_id"]=="2025-09-02"]["barRV"].round(6).tolist()
    assert got == [1.0, 1.0]

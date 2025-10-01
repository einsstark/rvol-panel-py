import pandas as pd

DAYS = 1  # compare against this many past sessions (testing tiny sample)

def load_csv(path: str) -> pd.DataFrame:
    need = {"timestamp","open","high","low","close","volume"}
    df = pd.read_csv(path)
    missing = need - set(df.columns)
    if missing:
        raise ValueError(f"CSV missing: {missing}")
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp").reset_index(drop=True)
    return df

def add_session_and_index(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["session_id"] = out["timestamp"].dt.date.astype(str)  # RTH = calendar day baseline
    out["bar_idx"] = out.groupby("session_id").cumcount()
    out["cum_volume"] = out.groupby("session_id")["volume"].cumsum()
    return out

def compute_rvol_simple(df: pd.DataFrame, days: int = DAYS) -> pd.DataFrame:
    out = df.copy()
    sessions = out["session_id"].unique().tolist()

    # build per-session frames indexed by bar_idx
    sess_frames = {s: out[out["session_id"] == s].set_index("bar_idx") for s in sessions}
    last_idx = {s: int(sf.index.max()) for s, sf in sess_frames.items()}

    out["hist_bar_avg"] = 0.0
    out["hist_cum_avg"] = 0.0
    out["barRV"] = 0.0
    out["cumRV"] = 0.0

    for i, s in enumerate(sessions):
        cur_mask = out["session_id"] == s
        k_slots = out.loc[cur_mask, "bar_idx"].to_numpy()

        past = sessions[max(0, i - days): i]
        if not past:
            continue  # here, no history 

        p = past[-1]                         # previous session (N=1 for now)
        plast = last_idx[p]                  # last bar index of previous session
        prev = sess_frames[p]

        # clamp k to previous day's last index if needed
        k_clamped = [min(int(k), plast) for k in k_slots]
        picked_bar = [float(prev.at[kc, "volume"]) for kc in k_clamped]
        picked_cum = [float(prev.at[kc, "cum_volume"]) for kc in k_clamped]

        out.loc[cur_mask, "hist_bar_avg"] = picked_bar
        out.loc[cur_mask, "hist_cum_avg"] = picked_cum

        # avoid divide by zero 
        hv = out.loc[cur_mask, "hist_bar_avg"].replace(0, pd.NA)
        hc = out.loc[cur_mask, "hist_cum_avg"].replace(0, pd.NA)
        out.loc[cur_mask, "barRV"] = out.loc[cur_mask, "volume"].div(hv).fillna(0.0)
        out.loc[cur_mask, "cumRV"] = out.loc[cur_mask, "cum_volume"].div(hc).fillna(0.0)

    return out

compute_rvol = compute_rvol_simple  # alias for tests

def main():
    df = load_csv("sample_intraday.csv")
    df = add_session_and_index(df)
    df = compute_rvol_simple(df, days=DAYS)
    last = df["session_id"].iloc[-1]
    print(
        df[df["session_id"] == last][
            ["timestamp","volume","bar_idx","cum_volume","hist_bar_avg","hist_cum_avg","barRV","cumRV"]
        ].to_string(index=False)
    )

if __name__ == "__main__":
    main()

from pathlib import Path

import h5py
import pandas as pd


RAW_CODES = [
    "000725",
    "601222",
    "600519",
    "600600",
    "600059",
    "600887",
    "000895",
    "600315",
    "601888",
    "600138",
    "002033",
    "000069",
    "600535",
    "000423",
    "600436",
    "000538",
    "600085",
    "600332",
    "600276",
    "600161",
    "300122",
    "300142",
    "600111",
    "600456",
    "601088",
    "601318",
    "600030",
    "600036",
    "600016",
    "600000",
]


def to_order_book_id(code):
    if code.startswith("6"):
        return "{}.XSHG".format(code)
    return "{}.XSHE".format(code)


def main():
    bundle_path = Path.home() / ".rqalpha" / "bundle" / "stocks.h5"
    output_dir = Path(__file__).resolve().parents[1] / "outputs" / "watchlist_data"
    output_dir.mkdir(parents=True, exist_ok=True)

    watchlist = [to_order_book_id(code) for code in RAW_CODES]
    all_frames = []
    summary = []

    with h5py.File(bundle_path, "r") as h5:
        for order_book_id in watchlist:
            if order_book_id not in h5:
                summary.append(
                    {
                        "order_book_id": order_book_id,
                        "rows": 0,
                        "start_date": None,
                        "end_date": None,
                        "status": "missing_in_bundle",
                    }
                )
                continue

            data = pd.DataFrame(h5[order_book_id][:])
            if data.empty:
                summary.append(
                    {
                        "order_book_id": order_book_id,
                        "rows": 0,
                        "start_date": None,
                        "end_date": None,
                        "status": "empty",
                    }
                )
                continue

            data["date"] = pd.to_datetime(
                data["datetime"].astype(str), format="%Y%m%d%H%M%S"
            ).dt.date

            data = data[
                [
                    "date",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "total_turnover",
                ]
            ]
            data.to_csv(output_dir / "{}_daily.csv".format(order_book_id), index=False)

            tagged = data.copy()
            tagged.insert(0, "order_book_id", order_book_id)
            all_frames.append(tagged)

            summary.append(
                {
                    "order_book_id": order_book_id,
                    "rows": len(data),
                    "start_date": data["date"].iloc[0],
                    "end_date": data["date"].iloc[-1],
                    "status": "ok",
                }
            )

    pd.DataFrame(summary).to_csv(output_dir / "summary.csv", index=False)
    if all_frames:
        pd.concat(all_frames, ignore_index=True).to_csv(
            output_dir / "watchlist_daily_all.csv", index=False
        )

    print("export finished")
    print("output_dir =", output_dir)
    print("summary_file =", output_dir / "summary.csv")
    print("all_data_file =", output_dir / "watchlist_daily_all.csv")


if __name__ == "__main__":
    main()

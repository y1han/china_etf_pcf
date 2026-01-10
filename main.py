import json
import time
from pathlib import Path
from datetime import datetime
import urllib.request
import shutil

import polars as pl
import akshare as ak
from tqdm import tqdm


class ETFPCFFetcher(object):
    time_gap: float = 1.1

    def __init__(self):
        self.trade_date: str = pl.DataFrame(ak.tool_trade_date_hist_sina()).filter(
            pl.col("trade_date") <= datetime.now().date()
        )["trade_date"][-1].strftime("%Y%m%d")
        self.file_path: Path = Path("temp") / self.trade_date
        if not self.file_path.exists():
            self.file_path.mkdir(exist_ok=True, parents=True)

    @staticmethod
    def get_fund_list_df() -> pl.DataFrame:
        return pl.DataFrame(ak.fund_etf_category_sina("ETF基金")).with_columns(
            pl.col("代码").str.slice(2, ) + pl.lit(".") + pl.col("代码").str.slice(0, 2).str.to_uppercase()
        )

    @staticmethod
    def get_sse_pcf_url(symbol: str) -> str:
        return f"https://query.sse.com.cn/etfDownload/downloadETF2Bulletin.do?fundCode={symbol}"

    @staticmethod
    def get_szse_pcf_url(symbol: str, trade_date: str) -> str:
        return f"https://reportdocs.static.szse.cn/files/text/ETFDown/pcf_{symbol}_{trade_date}.xml"

    def get_pcf_files(self, symbol_list: list[str], *, location: str):
        location_path = self.file_path / location
        if not location_path.exists():
            location_path.mkdir(exist_ok=True, parents=True)
        for symbol in tqdm(symbol_list):
            start_time = datetime.now()
            if location == "SH":
                urllib.request.urlretrieve(self.get_sse_pcf_url(symbol.split(".")[0]), location_path / f"{symbol}.xml")
            elif location == "SZ":
                urllib.request.urlretrieve(self.get_szse_pcf_url(symbol.split(".")[0], self.trade_date),
                                           location_path / f"{symbol}.xml")
            end_time = datetime.now()
            run_seconds = (end_time - start_time).total_seconds()
            if run_seconds < self.time_gap:
                time.sleep(self.time_gap - run_seconds)

    def compress_into_zip_file(self):
        shutil.make_archive(self.trade_date, "zip", self.file_path)

    def generate_github_release_metadata(self, fund_list_len: int):
        data = {
            "date": self.trade_date,
            "fetched_zip_path": self.trade_date + ".zip",
            "release_body": (
                f"# [{self.trade_date}] 沪深ETF PCF清单获取数量：{fund_list_len}"
            ),
        }
        with open("data.json", "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

    def run_today(self):
        # fund_list_df = self.get_fund_list_df()
        # fund_list_df_sse = fund_list_df.filter(pl.col("代码").str.ends_with("SH"))
        # fund_list_df_szse = fund_list_df.filter(pl.col("代码").str.ends_with("SZ"))
        # self.get_pcf_files(fund_list_df_sse["代码"].to_list(), location="SH")
        # self.get_pcf_files(fund_list_df_szse["代码"].to_list(), location="SZ")

        # Test
        self.get_pcf_files(["588000.SH", "588080.SH"], location="SH")
        self.get_pcf_files(["159915.SZ", "159998.SZ"], location="SZ")

        self.compress_into_zip_file()
        self.generate_github_release_metadata(4)
        return


if __name__ == '__main__':
    epf = ETFPCFFetcher()
    today_date = datetime.now().strftime("%Y%m%d")
    epf.run_today()
    # if epf.trade_date == today_date:
    #     epf.run_today()
    # else:
    #     raise AttributeError(f"{today_date} Not Trading Day!")

import json
import time
import ast
from pathlib import Path
from datetime import datetime
import urllib.request
import shutil
import xml
from xml.etree import ElementTree
import xmltodict

import polars as pl
import akshare as ak
from tqdm import tqdm

from utils.schema import InfoSchema, ComponentSchema
from utils.mapping import Mapping


class ETFPCFFetcher(object):
    time_gap: float = 1.1
    sse_tag: str = "SH"
    szse_tag: str = "SZ"

    def __init__(self, trade_date: str | None = None):
        self.trade_date: str = pl.DataFrame(ak.tool_trade_date_hist_sina()).filter(
            pl.col("trade_date") <= datetime.now().date()
        )["trade_date"][-1].strftime("%Y%m%d") if trade_date is None else trade_date
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

    @staticmethod
    def auto_cast_str(val):
        # Try fails if cannot eval, therefore is string
        try:
            val = ast.literal_eval(val)
        except Exception:
            pass
        return val

    @classmethod
    def xml_postprocessor(cls, _, key, value):
        # XML standard requires lower case bools
        if key in ["InstrumentID", "UnderlyingSecurityID", "UnderlyingSecurityIDSource"]: return key, value
        if value == "true": value = "True"
        if value == "false": value = "False"
        return key, cls.auto_cast_str(value)

    def get_pcf_files(self, symbol_list: list[str], *, location: str) -> None:
        location_path = self.file_path / location
        if not location_path.exists():
            location_path.mkdir(exist_ok=True, parents=True)
        for symbol in tqdm(symbol_list):
            start_time = datetime.now()
            if location == self.sse_tag:
                urllib.request.urlretrieve(self.get_sse_pcf_url(symbol.split(".")[0]), location_path / f"{symbol}.xml")
            elif location == self.szse_tag:
                urllib.request.urlretrieve(self.get_szse_pcf_url(symbol.split(".")[0], self.trade_date),
                                           location_path / f"{symbol}.xml")
            end_time = datetime.now()
            run_seconds = (end_time - start_time).total_seconds()
            if run_seconds < self.time_gap:
                time.sleep(self.time_gap - run_seconds)

    def compress_into_zip_file(self) -> None:
        shutil.make_archive(self.trade_date, "zip", self.file_path)

    def generate_github_release_metadata(self, fund_list_len: int) -> None:
        data = {
            "date": self.trade_date,
            "release_body": (
                f"### [{self.trade_date}] 沪深ETF PCF清单获取数量：{fund_list_len}"
            ),
        }
        with open("data.json", "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

    def aggregate_data_sse(self) -> tuple[pl.DataFrame, pl.DataFrame]:
        files = self.file_path.glob(f"{self.sse_tag}/*.xml")

        info_data = []
        component_data = []

        for file in files:
            try:
                tree = ElementTree.parse(file)
                xml_data = tree.getroot()
                xml_str = ElementTree.tostring(xml_data, encoding="utf-8", method="xml")

                data_dict = dict(xmltodict.parse(xml_str, postprocessor=self.xml_postprocessor))[
                    "SSEPortfolioCompositionFile"]
                component_dict = data_dict.pop("ComponentList")["Component"]
                component_dict = pl.from_records(
                    component_dict if isinstance(component_dict, list) else [component_dict]
                ).with_columns(
                    FundInstrumentID=pl.lit(data_dict["FundInstrumentID"]),
                    TradingDay=pl.lit(data_dict["TradingDay"])
                )
            except xml.etree.ElementTree.ParseError as e:
                print(f"{e}: {file}, Skipping legacy files")
                # file_text = sse_file.read_text(encoding="GB18030")
                continue

            info_data.append(data_dict)
            component_data.append(component_dict)
        info_data = pl.from_records(info_data).with_columns(
            pl.col("FundInstrumentID").cast(pl.String).str.zfill(6) + pl.lit(f".{self.sse_tag}")
        )
        component_data = pl.concat(component_data, how="diagonal_relaxed").with_columns(
            pl.col("InstrumentID") +
            pl.when(pl.col("UnderlyingSecurityID") == "9999").then(
                pl.lit("")
            ).otherwise(
                pl.lit(".") + pl.col("UnderlyingSecurityID").replace(
                    Mapping.exchange_code_map
                )
            ),
            pl.col("FundInstrumentID").cast(pl.String).str.zfill(6) + pl.lit(f".{self.sse_tag}")
        )
        return info_data, component_data

    def aggregate_data_szse(self) -> tuple[pl.DataFrame, pl.DataFrame]:
        files = self.file_path.glob(f"{self.szse_tag}/*.xml")

        info_data = []
        component_data = []

        for file in files:
            ElementTree.register_namespace("", "http://ts.szse.cn/Fund")
            tree = ElementTree.parse(file)
            xml_data = tree.getroot()
            xml_str = ElementTree.tostring(xml_data, encoding="utf-8", method="xml")

            data_dict = dict(xmltodict.parse(xml_str, postprocessor=self.xml_postprocessor))["PCFFile"]
            component_dict = data_dict.pop("Components")["Component"]
            component_dict = pl.from_records(
                component_dict if isinstance(component_dict, list) else [component_dict]
            ).with_columns(
                SecurityID=pl.lit(data_dict["SecurityID"]),
                TradingDay=pl.lit(data_dict["TradingDay"])
            )

            info_data.append(data_dict)
            component_data.append(component_dict)
        info_data = pl.from_records(info_data).with_columns(
            pl.col("SecurityID").cast(pl.String).str.zfill(6) + pl.lit(f".{self.szse_tag}")
        ).drop(["@xmlns", "Version"])
        component_data = pl.concat(component_data, how="diagonal_relaxed").with_columns(
            pl.col("UnderlyingSecurityID") +
            pl.when(pl.col("UnderlyingSecurityIDSource") == "9999").then(
                pl.lit("")
            ).otherwise(
                pl.lit(".") + pl.col("UnderlyingSecurityIDSource").replace(
                    Mapping.exchange_code_map
                )
            ),
            pl.col("SecurityID").cast(pl.String).str.zfill(6) + pl.lit(f".{self.szse_tag}")
        )
        return info_data, component_data

    def aggregate_data(self):
        sse_info, sse_component = self.aggregate_data_sse()
        szse_info, szse_component = self.aggregate_data_szse()

        sse_info.write_parquet(f"{self.trade_date}_{self.sse_tag}_INFO_RAW.parquet")
        sse_component.write_parquet(f"{self.trade_date}_{self.sse_tag}_COMPONENT_RAW.parquet")
        szse_info.write_parquet(f"{self.trade_date}_{self.szse_tag}_INFO_RAW.parquet")
        szse_component.write_parquet(f"{self.trade_date}_{self.szse_tag}_COMPONENT_RAW.parquet")

        info_df = self.aggregate_fund_info(sse_info, szse_info)
        comp_df = self.aggregate_fund_components(sse_component, szse_component)
        info_df.write_parquet(f"{self.trade_date}_INFO.parquet")
        comp_df.write_parquet(f"{self.trade_date}_COMPONENT.parquet")
        return

    @staticmethod
    def aggregate_fund_info(info_sh: pl.DataFrame, info_sz: pl.DataFrame) -> pl.DataFrame:
        tmp_info_sh = Mapping.clean_data(info_sh, Mapping.info_sh_map).with_columns(
            (pl.col(InfoSchema.creation_redemption_status.name).is_in([1, 2])).cast(pl.Int64).alias(
                InfoSchema.allow_creation.name),
            (pl.col(InfoSchema.creation_redemption_status.name).is_in([1, 3])).cast(pl.Int64).alias(
                InfoSchema.allow_redemption.name),
        ).drop(InfoSchema.creation_redemption_status.name)
        tmp_info_sz = Mapping.clean_data(info_sz, Mapping.info_sz_map).with_columns(
            pl.col("^.*creation_limit.*$").replace(0, None),
            pl.col("^.*redemption_limit.*$").replace(0, None),
            (pl.col(InfoSchema.publish_iopv_flag.name) == "Y").cast(pl.Int64),
            (pl.col(InfoSchema.allow_creation.name) == "Y").cast(pl.Int64),
            (pl.col(InfoSchema.allow_redemption.name) == "Y").cast(pl.Int64),
        )
        return pl.concat([tmp_info_sh, tmp_info_sz], how="diagonal_relaxed")

    @staticmethod
    def aggregate_fund_components(comp_sh: pl.DataFrame, comp_sz: pl.DataFrame) -> pl.DataFrame:
        tmp_comp_sh = Mapping.clean_data(comp_sh, Mapping.comp_sh_map).with_columns(
            pl.col(ComponentSchema.creation_substitution_cash_amount.name).alias(
                ComponentSchema.redemption_substitution_cash_amount.name)
        )
        tmp_comp_sz = Mapping.clean_data(comp_sz, Mapping.comp_sz_map)
        return pl.concat([tmp_comp_sh, tmp_comp_sz], how="diagonal_relaxed")

    def run_today(self):
        fund_list_df = self.get_fund_list_df()
        fund_list_df_sse = fund_list_df.filter(pl.col("代码").str.ends_with(self.sse_tag))
        fund_list_df_szse = fund_list_df.filter(pl.col("代码").str.ends_with(self.szse_tag))
        self.get_pcf_files(fund_list_df_sse["代码"].to_list(), location=self.sse_tag)
        self.get_pcf_files(fund_list_df_szse["代码"].to_list(), location=self.szse_tag)
        self.compress_into_zip_file()

        self.aggregate_data()
        self.generate_github_release_metadata(len(fund_list_df))
        return


if __name__ == '__main__':
    epf = ETFPCFFetcher()
    today_date = datetime.now().strftime("%Y%m%d")
    if epf.trade_date == today_date:
        epf.run_today()
    else:
        raise AttributeError(f"{today_date} Not Trading Day!")

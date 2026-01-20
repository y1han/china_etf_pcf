import polars as pl
from .schema import InfoSchema, ComponentSchema


class Mapping(object):
    exchange_code_map = {
        "101": "SH",
        "102": "SZ",
        "103": "HK",
        "105": "CFETS",
        "106": "BJ",
        "9999": "Other"
    }

    info_sh_map = {
        'FundInstrumentID': InfoSchema.fund_code,
        'TradingDay': InfoSchema.trade_date,
        'PreTradingDay': InfoSchema.previous_trade_date,
        'PreCashComponent': InfoSchema.previous_cash_component,
        'NAVperCU': InfoSchema.previous_nav_per_creation_redemption_unit,
        'NAV': InfoSchema.previous_nav_per_unit,
        'EstimatedCashComponent': InfoSchema.estimated_cash_component,
        'MaxCashRatio': InfoSchema.max_cash_ratio,
        'CreationLimit': InfoSchema.creation_limit,
        'RedemptionLimit': InfoSchema.redemption_limit,
        'NetCreationLimit': InfoSchema.net_creation_limit,
        'NetRedemptionLimit': InfoSchema.net_redemption_limit,
        'CreationLimitPerAcct': InfoSchema.creation_limit_per_account,
        'RedemptionLimitPerAcct': InfoSchema.redemption_limit_per_account,
        'NetCreationLimitPerAcct': InfoSchema.net_creation_limit_per_account,
        'NetRedemptionLimitPerAcct': InfoSchema.net_redemption_limit_per_account,
        'PublishIOPVFlag': InfoSchema.publish_iopv_flag,
        'CreationRedemptionUnit': InfoSchema.creation_redemption_unit,
        'CreationRedemptionSwitch': InfoSchema.creation_redemption_status,
        'CreationRedemptionMechanism': InfoSchema.creation_redemption_mechanism,
        'RecordNumber': InfoSchema.component_quantity,
    }

    info_sz_map = {
        'SecurityID': InfoSchema.fund_code,
        'UnderlyingSecurityID': InfoSchema.underlying_code,
        'TradingDay': InfoSchema.trade_date,
        'PreTradingDay': InfoSchema.previous_trade_date,
        'CashComponent': InfoSchema.previous_cash_component,
        'NAVperCU': InfoSchema.previous_nav_per_creation_redemption_unit,
        'NAV': InfoSchema.previous_nav_per_unit,
        'EstimateCashComponent': InfoSchema.estimated_cash_component,
        'MaxCashRatio': InfoSchema.max_cash_ratio,
        'CreationLimit': InfoSchema.creation_limit,
        'RedemptionLimit': InfoSchema.redemption_limit,
        'NetCreationLimit': InfoSchema.net_creation_limit,
        'NetRedemptionLimit': InfoSchema.net_redemption_limit,
        'CreationLimitPerUser': InfoSchema.creation_limit_per_account,
        'RedemptionLimitPerUser': InfoSchema.redemption_limit_per_account,
        'NetCreationLimitPerUser': InfoSchema.net_creation_limit_per_account,
        'NetRedemptionLimitPerUser': InfoSchema.net_redemption_limit_per_account,
        'Publish': InfoSchema.publish_iopv_flag,
        'CreationRedemptionUnit': InfoSchema.creation_redemption_unit,
        'Creation': InfoSchema.allow_creation,
        'Redemption': InfoSchema.allow_redemption,
        'TotalRecordNum': InfoSchema.component_quantity,
        'DividendPerCU': InfoSchema.dividend_per_creation_redemption_unit
    }

    comp_sh_map = {
        'FundInstrumentID': ComponentSchema.fund_code,
        'TradingDay': ComponentSchema.trade_date,
        'InstrumentID': ComponentSchema.security_code,
        'InstrumentName': ComponentSchema.security_name,
        'Quantity': ComponentSchema.quantity,
        'SubstitutionFlag': ComponentSchema.cash_substitution_flag,
        'CreationPremiumRate': ComponentSchema.creation_premium_rate,
        'RedemptionDiscountRate': ComponentSchema.redemption_discount_rate,
        'SubstitutionCashAmount': ComponentSchema.substitution_cash_amount,
        'UnderlyingSecurityID': ComponentSchema.underlying_security_market
    }

    comp_sz_map = {
        'SecurityID': ComponentSchema.fund_code,
        'TradingDay': ComponentSchema.trade_date,
        'UnderlyingSecurityID': ComponentSchema.security_code,
        'UnderlyingSymbol': ComponentSchema.security_name,
        'ComponentShare': ComponentSchema.quantity,
        'SubstituteFlag': ComponentSchema.cash_substitution_flag,
        'PremiumRatio': ComponentSchema.creation_premium_rate,
        'DiscountRatio': ComponentSchema.redemption_discount_rate,
        'CreationCashSubstitute': ComponentSchema.creation_substitution_cash_amount,
        'RedemptionCashSubstitute': ComponentSchema.redemption_substitution_cash_amount,
        'UnderlyingSecurityIDSource': ComponentSchema.underlying_security_market,
    }

    @staticmethod
    def clean_data(raw_data: pl.DataFrame, schema_map: dict[str, InfoSchema | ComponentSchema]) -> pl.DataFrame:
        return raw_data.select([key for key in schema_map.keys() if key in raw_data.columns]).rename(
            {k: v.name for k, v in schema_map.items()}, strict=False
        )

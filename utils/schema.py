from enum import Enum, auto

class InfoSchema(Enum):
    fund_code = auto()  # ETF代码
    underlying_code = auto()  # 底层指数代码
    trade_date = auto()  # 交易日

    previous_trade_date = auto()  # 前交易日
    previous_cash_component = auto()  # 现金差额
    previous_nav_per_creation_redemption_unit = auto()  # 最小申购赎回单位净值
    previous_nav_per_unit = auto()  # 基金份额净值

    estimated_cash_component = auto()  # 预估现金部分
    max_cash_ratio = auto()  # 现金替代比例上限
    creation_limit = auto()  # 当日申购份额上限
    redemption_limit = auto()  # 当日赎回份额上限
    net_creation_limit = auto()  # 当日净申购份额上限
    net_redemption_limit = auto()  # 当日净赎回上限
    creation_limit_per_account = auto()  # 单账户当日申购份额上限
    redemption_limit_per_account = auto()  # 单账户当日赎回份额上限
    net_creation_limit_per_account = auto()  # 单账户当日净申购份额上限
    net_redemption_limit_per_account = auto()  # 单账户当日净赎回上限
    publish_iopv_flag = auto()  # 是否公布IOPV 1=是 0=否
    creation_redemption_unit = auto()  # 最小申购赎回单位
    creation_redemption_status = auto()  # 申购赎回允许情况
    allow_creation = auto()  # 允许申购
    allow_redemption = auto()  # 允许赎回
    creation_redemption_mechanism = auto()  # 申购赎回模式
    component_quantity = auto()  # 成分数量
    dividend_per_creation_redemption_unit = auto()  # 最小申购赎回单位现金红利


class ComponentSchema(Enum):
    security_code = auto()  # 证券代码
    security_name = auto()  # 证券名称
    quantity = auto()  # 股票数量
    cash_substitution_flag = auto()  # 现金替代标志
    creation_premium_rate = auto()  # 申购现金替代溢价比例
    redemption_discount_rate = auto()  # 赎回现金替代折价比例
    substitution_cash_amount = auto()  # 替代金额（仅沪市）
    creation_substitution_cash_amount = auto()  # 申购替代金额（仅深市）
    redemption_substitution_cash_amount = auto()  # 赎回替代金额（仅深市）
    underlying_security_market = auto()  # 挂牌市场
    fund_code = auto()  # ETF代码
    trade_date = auto()  # 交易日
